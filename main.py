"""
main.py — Tableau Bulk PDF Exporter
=====================================
Exports one PDF per filter value, bundles them into a ZIP, returns the ZIP.

Architecture (for large batches like 800+ values):
  1. POST /export-pdf/start  → starts background job, returns job_id immediately
  2. GET  /export-pdf/progress/{job_id}  → poll for live progress
  3. GET  /export-pdf/download/{job_id}  → download the ZIP when done

Requirements:
  pip install fastapi uvicorn requests pypdf python-multipart

Run:
  uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
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
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────

MAX_CONCURRENT_DOWNLOADS = 8   # parallel PDF downloads per job
MAX_WORKERS              = 16  # thread pool size
JOB_TTL_MINUTES          = 60  # how long completed jobs are kept in memory

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(title="Tableau Bulk PDF Exporter", version="4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job store  {job_id: Job}
jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()


# ── Models ────────────────────────────────────────────────────────────────────

class ExportRequest(BaseModel):
    tableau_server: str
    site_name: str
    pat_name: str
    pat_secret: str
    view_id: str
    filter_field: str
    filter_values: list[str]

class LookupRequest(BaseModel):
    tableau_server: str
    site_name: str
    pat_name: str
    pat_secret: str


# ── Tableau helpers ────────────────────────────────────────────────────────────

def tableau_signin(server: str, site_name: str, pat_name: str, pat_secret: str) -> tuple[str, str]:
    """Returns (token, site_id)."""
    url  = f"{server}/api/3.19/auth/signin"
    resp = requests.post(url, json={
        "credentials": {
            "personalAccessTokenName":   pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {"contentUrl": site_name},
        }
    }, headers={"Accept": "application/json", "Content-Type": "application/json"}, timeout=30)

    if not resp.text:
        raise RuntimeError(f"Empty Tableau auth response. HTTP {resp.status_code}")
    if resp.status_code != 200:
        raise RuntimeError(f"Tableau auth failed ({resp.status_code}): {resp.text[:400]}")

    data = resp.json()
    return data["credentials"]["token"], data["credentials"]["site"]["id"]


def tableau_signout(server: str, token: str):
    try:
        requests.post(
            f"{server}/api/3.19/auth/signout",
            headers={"x-tableau-auth": token}, timeout=10
        )
    except Exception:
        pass


def safe_filename(value: str) -> str:
    """Sanitise a filter value so it's safe to use as a filename."""
    name = re.sub(r'[\\/*?:"<>|]', "_", value)
    return (name[:100] + ".pdf") if len(name) > 100 else f"{name}.pdf"


def download_one_pdf(
    server: str, site_id: str, view_id: str,
    token: str, filter_field: str, filter_value: str
) -> bytes:
    url = f"{server}/api/3.19/sites/{site_id}/views/{view_id}/pdf"
    resp = requests.get(
        url,
        params={"type": "A4", "orientation": "Portrait", f"vf_{filter_field}": filter_value},
        headers={"x-tableau-auth": token},
        timeout=90,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.content


# ── Background job logic ───────────────────────────────────────────────────────

def _run_export_job(job_id: str, req: ExportRequest):
    """
    Runs in a background thread.
    Downloads PDFs concurrently and writes them into a ZIP in memory.
    Updates the shared job dict with live progress.
    """
    def update(done=None, failed=None, status=None, error=None, zip_bytes=None):
        with jobs_lock:
            j = jobs[job_id]
            if done       is not None: j["done"]      = done
            if failed     is not None: j["failed"]    = failed
            if status     is not None: j["status"]    = status
            if error      is not None: j["error"]     = error
            if zip_bytes  is not None: j["zip_bytes"] = zip_bytes

    try:
        # Step 1 — Authenticate
        update(status="signing_in")
        token, site_id = tableau_signin(
            req.tableau_server, req.site_name,
            req.pat_name, req.pat_secret
        )

        total       = len(req.filter_values)
        done_count  = 0
        fail_count  = 0
        fail_names: list[str] = []

        # Step 2 — Download PDFs concurrently with a semaphore
        update(status="exporting")

        # We'll store results in order: list of (filter_value, bytes | None, error_msg)
        results: list[tuple[str, bytes | None, str]] = [None] * total

        semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)

        def fetch(index: int, value: str):
            nonlocal done_count, fail_count
            with semaphore:
                try:
                    pdf = download_one_pdf(
                        req.tableau_server, site_id, req.view_id,
                        token, req.filter_field, value
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

        # Step 3 — Package into ZIP
        update(status="zipping")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for (value, pdf_bytes, err) in results:
                if pdf_bytes:
                    zf.writestr(safe_filename(value), pdf_bytes)

        zip_buffer.seek(0)
        update(status="done", zip_bytes=zip_buffer.read())

    except Exception as e:
        update(status="error", error=f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}")
    finally:
        try:
            tableau_signout(req.tableau_server, token)
        except Exception:
            pass


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
def health():
    return {
        "status":  "ok",
        "version": "4.0",
        "active_jobs": len([j for j in jobs.values() if j["status"] not in ("done", "error")])
    }


@app.post("/export-pdf/start")
async def start_export(req: ExportRequest):
    """
    Starts a background export job.
    Returns immediately with a job_id the client can poll.
    """
    if not req.filter_values:
        raise HTTPException(400, "filter_values is empty")

    job_id = str(uuid.uuid4())
    total  = len(req.filter_values)

    with jobs_lock:
        jobs[job_id] = {
            "status":       "queued",    # queued | signing_in | exporting | zipping | done | error
            "total":        total,
            "done":         0,
            "failed":       0,
            "failed_names": [],
            "zip_bytes":    None,
            "error":        None,
            "created_at":   datetime.utcnow(),
        }

    # Kick off in background thread
    t = threading.Thread(target=_run_export_job, args=(job_id, req), daemon=True)
    t.start()

    return {"job_id": job_id, "total": total}


@app.get("/export-pdf/progress/{job_id}")
async def get_progress(job_id: str):
    """Poll this endpoint for live progress."""
    with jobs_lock:
        j = jobs.get(job_id)

    if j is None:
        raise HTTPException(404, "Job not found")

    # Don't leak zip_bytes in progress response
    return {
        "status":       j["status"],
        "total":        j["total"],
        "done":         j["done"],
        "failed":       j["failed"],
        "failed_names": j["failed_names"][:20],  # first 20 failures only
        "error":        j["error"],
        "pct":          round(j["done"] / j["total"] * 100) if j["total"] else 0,
    }


@app.get("/export-pdf/download/{job_id}")
async def download_zip(job_id: str):
    """Download the ZIP file once the job is done."""
    with jobs_lock:
        j = jobs.get(job_id)

    if j is None:
        raise HTTPException(404, "Job not found")
    if j["status"] == "error":
        raise HTTPException(500, j["error"] or "Export failed")
    if j["status"] != "done":
        raise HTTPException(409, f"Job not finished yet (status: {j['status']})")
    if not j["zip_bytes"]:
        raise HTTPException(500, "ZIP is empty — all exports failed")

    filename = f"tableau_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"

    # Clean up job after serving (save memory)
    with jobs_lock:
        zip_data = j["zip_bytes"]
        jobs[job_id]["zip_bytes"] = None  # free memory immediately after first download

    return StreamingResponse(
        io.BytesIO(zip_data),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.delete("/export-pdf/job/{job_id}")
async def cancel_job(job_id: str):
    """Clean up a job from memory."""
    with jobs_lock:
        if job_id in jobs:
            del jobs[job_id]
    return {"deleted": job_id}


@app.post("/get-view-id")
async def get_view_id(req: LookupRequest):
    """List all views in the site so you can find the right view_id."""
    token, site_id = tableau_signin(
        req.tableau_server, req.site_name, req.pat_name, req.pat_secret
    )
    try:
        headers = {"x-tableau-auth": token, "Accept": "application/json"}
        wbs_resp = requests.get(
            f"{req.tableau_server}/api/3.19/sites/{site_id}/workbooks",
            headers=headers, timeout=30
        )
        workbooks = wbs_resp.json().get("workbooks", {}).get("workbook", [])
        result = []
        for wb in workbooks:
            vr = requests.get(
                f"{req.tableau_server}/api/3.19/sites/{site_id}/workbooks/{wb['id']}/views",
                headers=headers, timeout=30
            )
            for v in vr.json().get("views", {}).get("view", []):
                result.append({"workbook": wb["name"], "view_name": v["name"], "view_id": v["id"]})
        return {"views": result}
    finally:
        tableau_signout(req.tableau_server, token)


# ── Periodic cleanup ──────────────────────────────────────────────────────────

def _cleanup_old_jobs():
    """Remove jobs older than JOB_TTL_MINUTES. Runs in a background thread."""
    import time
    while True:
        time.sleep(300)  # check every 5 min
        cutoff = datetime.utcnow() - timedelta(minutes=JOB_TTL_MINUTES)
        with jobs_lock:
            stale = [jid for jid, j in jobs.items() if j["created_at"] < cutoff]
            for jid in stale:
                del jobs[jid]
        if stale:
            print(f"Cleaned up {len(stale)} stale jobs")

threading.Thread(target=_cleanup_old_jobs, daemon=True).start()
