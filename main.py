from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import pypdf
import io

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ExportRequest(BaseModel):
    tableau_server: str       # e.g. https://prod-apsoutheast-a.online.tableau.com
    site_name: str            # e.g. mekariinsight
    pat_name: str             # Personal Access Token name
    pat_secret: str           # Personal Access Token secret
    view_id: str              # The view ID from Tableau (use /get-view-id to find it)
    filter_field: str         # e.g. "Ship Mode" (exact field name in Tableau)
    filter_values: list[str]  # e.g. ["First Class", "Second Class", ...]


class LookupRequest(BaseModel):
    tableau_server: str
    site_name: str
    pat_name: str
    pat_secret: str


# ── Auth helpers ──────────────────────────────────────────────────────────────

def tableau_signin(server: str, site_name: str, pat_name: str, pat_secret: str):
    url = f"{server}/api/3.19/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {"contentUrl": site_name}
        }
    }
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail=f"Tableau auth failed: {response.text}")

    data = response.json()
    token = data["credentials"]["token"]
    site_id = data["credentials"]["site"]["id"]
    return token, site_id


def tableau_signout(server: str, token: str):
    requests.post(
        f"{server}/api/3.19/auth/signout",
        headers={"x-tableau-auth": token}
    )


# ── PDF helpers ───────────────────────────────────────────────────────────────

def download_view_pdf(server: str, site_id: str, view_id: str, token: str, filter_field: str, filter_value: str) -> bytes:
    url = f"{server}/api/3.19/sites/{site_id}/views/{view_id}/pdf"
    params = {
        "type": "A4",
        "orientation": "Portrait",
        f"vf_{filter_field}": filter_value
    }
    headers = {"x-tableau-auth": token}
    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get PDF for '{filter_value}': {response.text}"
        )
    return response.content


def merge_pdfs(pdf_bytes_list: list[bytes]) -> bytes:
    merger = pypdf.PdfWriter()
    for pdf_bytes in pdf_bytes_list:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            merger.add_page(page)

    output = io.BytesIO()
    merger.write(output)
    output.seek(0)
    return output.read()


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
def health_check():
    return {"status": "ok", "message": "Tableau PDF Exporter is running"}


@app.post("/get-view-id")
async def get_view_id(req: LookupRequest):
    token, site_id = tableau_signin(
        req.tableau_server,
        req.site_name,
        req.pat_name,
        req.pat_secret
    )
    try:
        url = f"{req.tableau_server}/api/3.19/sites/{site_id}/views"
        headers = {"x-tableau-auth": token}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=response.text)
        views = response.json().get("views", {}).get("view", [])
        return {
            "views": [
                {
                    "id": v["id"],
                    "name": v["name"],
                    "workbook": v.get("owner", {}).get("name", "")
                }
                for v in views
            ]
        }
    finally:
        tableau_signout(req.tableau_server, token)


@app.post("/export-pdf")
async def export_pdf(req: ExportRequest):
    """Bulk export — one PDF page per filter value, merged into a single file."""
    if not req.filter_values:
        raise HTTPException(status_code=400, detail="No filter values provided")

    token, site_id = tableau_signin(
        req.tableau_server,
        req.site_name,
        req.pat_name,
        req.pat_secret
    )

    try:
        pdf_list = []
        failed = []

        for value in req.filter_values:
            try:
                pdf_bytes = download_view_pdf(
                    req.tableau_server,
                    site_id,
                    req.view_id,
                    token,
                    req.filter_field,
                    value
                )
                pdf_list.append(pdf_bytes)
            except Exception as e:
                failed.append(f"{value}: {str(e)}")
                continue

        if not pdf_list:
            raise HTTPException(status_code=500, detail=f"All exports failed: {failed}")

        merged = merge_pdfs(pdf_list)

        return StreamingResponse(
            io.BytesIO(merged),
            media_type="application/pdf",
            headers={
                "Content-Disposition": "attachment; filename=bulk_export.pdf",
                "X-Failed-Exports": str(len(failed)),
                "X-Successful-Exports": str(len(pdf_list)),
            }
        )

    finally:
        tableau_signout(req.tableau_server, token)
