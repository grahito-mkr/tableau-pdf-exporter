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
    tableau_server: str      # e.g. https://prod-apnortheast-a.online.tableau.com
    site_name: str           # e.g. mekari (from the URL)
    pat_name: str            # Personal Access Token name
    pat_secret: str          # Personal Access Token secret
    view_id: str             # The view/sheet ID from Tableau
    filter_field: str        # e.g. "Employee Name" (exact field name in Tableau)
    filter_values: list[str] # e.g. ["John Doe", "Jane Smith", ...]


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
            detail=f"Failed to get PDF for {filter_value}: {response.text}"
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


@app.get("/")
def health_check():
    return {"status": "ok", "message": "Tableau PDF Exporter is running"}


@app.post("/export-pdf")
async def export_pdf(req: ExportRequest):
    if not req.filter_values:
        raise HTTPException(status_code=400, detail="No filter values provided")

    # 1. Sign in to Tableau
    token, site_id = tableau_signin(
        req.tableau_server,
        req.site_name,
        req.pat_name,
        req.pat_secret
    )

    try:
        # 2. Download PDF for each filter value
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

        # 3. Merge all PDFs
        merged = merge_pdfs(pdf_list)

        # 4. Return as downloadable file
        return StreamingResponse(
            io.BytesIO(merged),
            media_type="application/pdf",
            headers={
                "Content-Disposition": "attachment; filename=employee_profiles.pdf",
                "X-Failed-Exports": str(len(failed)),
            }
        )

    finally:
        # Always sign out
        tableau_signout(req.tableau_server, token)
