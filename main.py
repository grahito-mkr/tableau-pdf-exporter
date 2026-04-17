"""
main.py — Tableau Bulk PDF Exporter v5
========================================
Exports one PDF per filter value, bundles into a ZIP file.
Supports custom vizWidth/vizHeight for any dashboard size.

Deploy on Railway:
  - Procfile:          web: uvicorn main:app --host 0.0.0.0 --port $PORT
  - requirements.txt:  fastapi uvicorn requests pypdf python-multipart

Endpoints:
  POST /export-pdf/start          → start job, returns job_id
  GET  /export-pdf/progress/{id}  → poll live progress
  GET  /export-pdf/download/{id}  → download ZIP when done
  GET  /                          → health check
"""

import io
import re
import uuid
import zipfile
import threading
import traceback
import concurrent.futures
from datetime import datetime, timedelta

import requests
import pypdf
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────

MAX_CONCURRENT_DOWNLOADS = 8
MAX_WORKERS              = 16
JOB_TTL_MINUTES          = 60

# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="Tableau Bulk PDF Exporter", version="5.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()


# ── Request models ────────────────────────────────────────────────────────────

class ExportRequest(BaseModel):
    tableau_server: str
    site_name:      str
    pat_name:       str
    pat_secret:     str
    view_id:        str
    filter_field:   str
    filter_values:  list[str]
    viz_width:      int = 1400
    viz_height:     int = 900


class LookupRequest(BaseModel):
    tableau_server: str
    site_name:      str
    pat_name:       str
    pat_secret:     str


# ── Tableau helpers ───────────────────────────────────────────────────────────

def tableau_signin(server, site_name, pat_name, pat_secret):
    resp = requests.post(
        f"{server}/api/3.19/auth/signin",
        json={
            "credentials": {
                "personalAccessTokenName":   pat_name,
                "personalAccessTokenSecret": pat_secret,
                "site": {"contentUrl": site_name},
            }
        },
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=30,
    )
    if not resp.text:
        raise RuntimeError(f"Empty Tableau auth response. HTTP {resp.status_code}")
    if resp.status_code != 200:
        raise RuntimeError(f"Tableau auth failed ({resp.status_code}): {resp.text[:400]}")
    data = resp.json()
    return data["credentials"]["token"], data["credentials"]["site"]["id"]


def tableau_signout(server, token):
    try:
        requests.post(
            f"{server}/api/3.19/auth/signout",
            headers={"x-tableau-auth": token},
            timeout=10,
        )
    except Exception:
        pass


def safe_filename(value):
    name = re.sub(r'[\\/*?:"<>|]', "_", value).strip()
    return (name[:100] + ".pdf") if len(name) > 100 else f"{name}.pdf"


def download_one_pdf(server, site_id, view_id, token,
                     filter_field, filter_value, viz_width, viz_height):
    """
    Uses A4 paper size. Orientation is chosen automatically:
    - viz_width > viz_height  → Landscape  (wide dashboards)
    - viz_width <= viz_height → Portrait   (tall/narrow dashboards)
    """
    orientation = "Landscape" if viz_width > viz_height else "Portrait"
    resp = requests.get(
        f"{server}/api/3.19/sites/{site_id}/views/{view_id}/pdf",
        params={
            "type":        "A4",
            "orientation": orientation,
            f"vf_{filter_field}": filter_value,
        },
        headers={"x-tableau-auth": token},
        timeout=90,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.content


# ── Background job ────────────────────────────────────────────────────────────

def _run_export_job(job_id, req):
    token = None

    def update(**kwargs):
        with jobs_lock:
            jobs[job_id].update(kwargs)

    try:
        update(status="signing_in")
        token, site_id = tableau_signin(
            req.tableau_server, req.site_name,
            req.pat_name, req.pat_secret,
        )

        total   = len(req.filter_values)
        results = [None] * total
        semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)

        update(status="exporting")

        def fetch(index, value):
            with semaphore:
                try:
                    pdf = download_one_pdf(
                        req.tableau_server, site_id, req.view_id,
                        token, req.filter_field, value,
                        req.viz_width, req.viz_height,
                    )
                    results[index] = (value, pdf, "")
                except Exception as e:
                    results[index] = (value, None, str(e))
                    with jobs_lock:
                        jobs[job_id]["failed"] += 1
                        jobs[job_id]["failed_names"].append(value)
                finally:
                    with jobs_lock:
                        jobs[job_id]["done"] += 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(fetch, i, v) for i, v in enumerate(req.filter_values)]
            concurrent.futures.wait(futures)

        update(status="zipping")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for value, pdf_bytes, _ in results:
                if pdf_bytes:
                    zf.writestr(safe_filename(value), pdf_bytes)

        zip_buffer.seek(0)
        update(status="done", zip_bytes=zip_buffer.read())

    except Exception as e:
        update(status="error", error=f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}")
    finally:
        if token:
            tableau_signout(req.tableau_server, token)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
def health():
    active = sum(1 for j in jobs.values() if j["status"] not in ("done", "error"))
    return {"status": "ok", "version": "5.0", "active_jobs": active}


@app.post("/export-pdf/start")
async def start_export(req: ExportRequest):
    if not req.filter_values:
        raise HTTPException(400, "filter_values is empty")

    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "status":       "queued",
            "total":        len(req.filter_values),
            "done":         0,
            "failed":       0,
            "failed_names": [],
            "zip_bytes":    None,
            "error":        None,
            "created_at":   datetime.utcnow(),
        }

    threading.Thread(target=_run_export_job, args=(job_id, req), daemon=True).start()
    return {"job_id": job_id, "total": len(req.filter_values)}


@app.get("/export-pdf/progress/{job_id}")
async def get_progress(job_id: str):
    with jobs_lock:
        j = jobs.get(job_id)
    if j is None:
        raise HTTPException(404, "Job not found")
    return {
        "status":       j["status"],
        "total":        j["total"],
        "done":         j["done"],
        "failed":       j["failed"],
        "failed_names": j["failed_names"][:20],
        "error":        j["error"],
        "pct":          round(j["done"] / j["total"] * 100) if j["total"] else 0,
    }


@app.get("/export-pdf/download/{job_id}")
async def download_zip(job_id: str):
    with jobs_lock:
        j = jobs.get(job_id)
    if j is None:
        raise HTTPException(404, "Job not found")
    if j["status"] == "error":
        raise HTTPException(500, j["error"] or "Export failed")
    if j["status"] != "done":
        raise HTTPException(409, f"Job not ready yet (status: {j['status']})")
    if not j["zip_bytes"]:
        raise HTTPException(500, "ZIP is empty — all exports may have failed")

    filename = f"tableau_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
    with jobs_lock:
        zip_data = j["zip_bytes"]
        jobs[job_id]["zip_bytes"] = None

    return StreamingResponse(
        io.BytesIO(zip_data),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.delete("/export-pdf/job/{job_id}")
async def delete_job(job_id: str):
    with jobs_lock:
        jobs.pop(job_id, None)
    return {"deleted": job_id}


@app.post("/get-view-id")
async def get_view_id(req: LookupRequest):
    token, site_id = tableau_signin(
        req.tableau_server, req.site_name,
        req.pat_name, req.pat_secret,
    )
    try:
        headers = {"x-tableau-auth": token, "Accept": "application/json"}
        wbs = requests.get(
            f"{req.tableau_server}/api/3.19/sites/{site_id}/workbooks",
            headers=headers, timeout=30,
        ).json().get("workbooks", {}).get("workbook", [])

        result = []
        for wb in wbs:
            views = requests.get(
                f"{req.tableau_server}/api/3.19/sites/{site_id}/workbooks/{wb['id']}/views",
                headers=headers, timeout=30,
            ).json().get("views", {}).get("view", [])
            for v in views:
                result.append({
                    "workbook":  wb["name"],
                    "view_name": v["name"],
                    "view_id":   v["id"],
                })
        return {"views": result}
    finally:
        tableau_signout(req.tableau_server, token)


# ── Cleanup thread ────────────────────────────────────────────────────────────

def _cleanup_loop():
    import time
    while True:
        time.sleep(300)
        cutoff = datetime.utcnow() - timedelta(minutes=JOB_TTL_MINUTES)
        with jobs_lock:
            stale = [jid for jid, j in jobs.items() if j["created_at"] < cutoff]
            for jid in stale:
                del jobs[jid]
        if stale:
            print(f"Cleaned up {len(stale)} stale jobs")

threading.Thread(target=_cleanup_loop, daemon=True).start()
