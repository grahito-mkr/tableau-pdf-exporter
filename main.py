"""
main.py — Tableau Bulk PDF Exporter (Multi-Client)
====================================================
One deployment serves all clients.
Each client is identified by their Tableau workbook name.

To add a new client:
  1. Add an entry to CLIENT_CONFIGS below using their exact Tableau workbook name
  2. Commit to GitHub → Railway auto-redeploys

Deploy on Railway:
  Procfile:         web: uvicorn main:app --host 0.0.0.0 --port $PORT
  requirements.txt: fastapi uvicorn requests pypdf python-multipart
"""

import io
import os
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

# ══════════════════════════════════════════════════════════════════════════════
#  CLIENT CONFIGS — add one entry per client
#  Key = exact Tableau workbook name (case-sensitive)
# ══════════════════════════════════════════════════════════════════════════════

# ── PAT credentials loaded from Railway environment variables ────────────────
# Never hardcode secrets here. Set them in Railway dashboard → Variables tab.
#
# Naming convention:  {CLIENT_KEY}_PAT_NAME  and  {CLIENT_KEY}_PAT_SECRET
# Example Railway vars for two clients:
#   MEKARI_PAT_NAME       = tableau-bulk-download
#   MEKARI_PAT_SECRET     = Xvm3oCPtRAaeYnzgwHBM5A==:gnEcc...
#   CLIENT_B_PAT_NAME     = their-pat-name
#   CLIENT_B_PAT_SECRET   = their-secret

def get_client_configs():
    """
    Builds the client config dict at request time (not at startup),
    so Railway environment variables are guaranteed to be available.
    """
    def env(key):
        val = os.environ.get(key)
        if not val:
            raise RuntimeError(
                f"Missing environment variable: {key} — "
                f"set it in Railway dashboard → Variables tab."
            )
        return val

    return {
        "Testing Ship Mode - Bulk Download": {
            "tableau_server": "https://prod-apsoutheast-a.online.tableau.com",
            "site_name":      "mekariinsight",
            "pat_name":       env("CLIENT_A_PAT_NAME"),
            "pat_secret":     env("CLIENT_A_PAT_SECRET"),
            "view_id":        "f7c4dfcd-da22-42f9-835b-e2ddeed7bffb",
            "filter_field":   "Customer Name",
            "orientation":    "Landscape",
        },

        # ── Add more clients below ────────────────────────────────────────────
        # "Client B Workbook Name": {
        #     "tableau_server": "https://prod-apsoutheast-a.online.tableau.com",
        #     "site_name":      "clientbsite",
        #     "pat_name":       env("CLIENT_B_PAT_NAME"),
        #     "pat_secret":     env("CLIENT_B_PAT_SECRET"),
        #     "view_id":        "their-view-id",
        #     "filter_field":   "Customer Name",
        #     "orientation":    "Portrait",
        # },
    }

# ══════════════════════════════════════════════════════════════════════════════

MAX_CONCURRENT_DOWNLOADS = 8
MAX_WORKERS              = 16
JOB_TTL_MINUTES          = 60

app = FastAPI(title="Tableau Bulk PDF Exporter", version="5.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()


# ── Models ────────────────────────────────────────────────────────────────────

class ExportRequest(BaseModel):
    tableau_server: str
    site_name:      str
    pat_name:       str
    pat_secret:     str
    view_id:        str
    filter_field:   str
    filter_values:  list[str]
    orientation:    str = "Landscape"


# ── Tableau helpers ───────────────────────────────────────────────────────────

def tableau_signin(server, site_name, pat_name, pat_secret):
    resp = requests.post(
        f"{server}/api/3.19/auth/signin",
        json={"credentials": {
            "personalAccessTokenName":   pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {"contentUrl": site_name},
        }},
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
        requests.post(f"{server}/api/3.19/auth/signout",
                      headers={"x-tableau-auth": token}, timeout=10)
    except Exception:
        pass


def safe_filename(value):
    name = re.sub(r'[\\/*?:"<>|]', "_", value).strip()
    return (name[:100] + ".pdf") if len(name) > 100 else f"{name}.pdf"


def download_one_pdf(server, site_id, view_id, token,
                     filter_field, filter_value, orientation="Landscape"):
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

        total     = len(req.filter_values)
        results   = [None] * total
        semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        update(status="exporting")

        def fetch(index, value):
            with semaphore:
                try:
                    pdf = download_one_pdf(
                        req.tableau_server, site_id, req.view_id,
                        token, req.filter_field, value, req.orientation,
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
    return {"status": "ok", "version": "5.0", "active_jobs": active,
            "registered_clients": list(get_client_configs().keys())}


@app.get("/config")
def get_config(workbook: str):
    """
    Returns the filter_field for a given workbook name.
    Called by the HTML on load to know which filter to read.
    e.g. GET /config?workbook=Testing+Ship+Mode+-+Bulk+Download
    """
    cfg = get_client_configs().get(workbook)
    if not cfg:
        raise HTTPException(404, f"No config found for workbook: '{workbook}'")
    return {"filter_field": cfg["filter_field"]}


@app.post("/export-pdf/start")
async def start_export(body: dict):
    """
    Start an export job. Looks up config by workbook name.
    Body: { "workbook_name": "...", "filter_values": [...] }
    """
    workbook_name = body.get("workbook_name", "")
    filter_values = body.get("filter_values", [])

    if not workbook_name:
        raise HTTPException(400, "workbook_name is required")
    if not filter_values:
        raise HTTPException(400, "filter_values is empty")

    cfg = get_client_configs().get(workbook_name)
    if not cfg:
        raise HTTPException(404, f"No config found for workbook: '{workbook_name}'")

    req = ExportRequest(
        tableau_server = cfg["tableau_server"],
        site_name      = cfg["site_name"],
        pat_name       = cfg["pat_name"],
        pat_secret     = cfg["pat_secret"],
        view_id        = cfg["view_id"],
        filter_field   = cfg["filter_field"],
        orientation    = cfg["orientation"],
        filter_values  = filter_values,
    )

    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "status":       "queued",
            "total":        len(filter_values),
            "done":         0,
            "failed":       0,
            "failed_names": [],
            "zip_bytes":    None,
            "error":        None,
            "created_at":   datetime.utcnow(),
        }

    threading.Thread(target=_run_export_job, args=(job_id, req), daemon=True).start()
    return {"job_id": job_id, "total": len(filter_values)}


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


# ── Cleanup ───────────────────────────────────────────────────────────────────

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
