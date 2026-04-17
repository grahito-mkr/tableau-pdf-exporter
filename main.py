from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import pypdf
import io
import traceback

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "traceback": traceback.format_exc()}
    )


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


def tableau_signin(server: str, site_name: str, pat_name: str, pat_secret: str):
    url = f"{server}/api/3.19/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {"contentUrl": site_name}
        }
    }
    response = requests.post(
        url, json=payload, timeout=30,
        headers={"Accept": "application/json", "Content-Type": "application/json"}
    )
    if not response.text:
        raise Exception(f"Empty response from Tableau. Status: {response.status_code}")
    if response.status_code != 200:
        raise Exception(f"Tableau auth failed ({response.status_code}): {response.text}")
    data = response.json()
    return data["credentials"]["token"], data["credentials"]["site"]["id"]


def tableau_signout(server: str, token: str):
    try:
        requests.post(
            f"{server}/api/3.19/auth/signout",
            headers={"x-tableau-auth": token}, timeout=10
        )
    except:
        pass


def download_view_pdf(server: str, site_id: str, view_id: str, token: str, filter_field: str, filter_value: str) -> bytes:
    url = f"{server}/api/3.19/sites/{site_id}/views/{view_id}/pdf"
    params = {
        "type": "A4",
        "orientation": "Portrait",
        f"vp_{filter_field}": filter_value
    }
    response = requests.get(url, params=params, headers={"x-tableau-auth": token}, timeout=60)
    if response.status_code != 200:
        raise Exception(f"PDF failed for '{filter_value}' ({response.status_code}): {response.text}")
    return response.content


def merge_pdfs(pdf_bytes_list: list[bytes]) -> bytes:
    merger = pypdf.PdfWriter()
    for pdf_bytes in pdf_bytes_list:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        # Take only first page per filter value — avoids duplicate sheets
        if len(reader.pages) > 0:
            merger.add_page(reader.pages[0])
    output = io.BytesIO()
    merger.write(output)
    output.seek(0)
    return output.read()


@app.get("/")
def health_check():
    return {"status": "ok", "message": "Tableau PDF Exporter is running", "version": "3"}


@app.post("/get-view-id")
async def get_view_id(req: LookupRequest):
    token, site_id = tableau_signin(req.tableau_server, req.site_name, req.pat_name, req.pat_secret)
    try:
        url = f"{req.tableau_server}/api/3.19/sites/{site_id}/workbooks"
        headers = {"x-tableau-auth": token, "Accept": "application/json"}
        response = requests.get(url, headers=headers, timeout=30)
        workbooks = response.json().get("workbooks", {}).get("workbook", [])
        result = []
        for wb in workbooks:
            wb_id = wb["id"]
            wb_name = wb["name"]
            views_url = f"{req.tableau_server}/api/3.19/sites/{site_id}/workbooks/{wb_id}/views"
            vr = requests.get(views_url, headers=headers, timeout=30)
            views = vr.json().get("views", {}).get("view", [])
            for v in views:
                result.append({"workbook": wb_name, "view_name": v["name"], "view_id": v["id"]})
        return {"views": result}
    finally:
        tableau_signout(req.tableau_server, token)


@app.post("/export-pdf")
async def export_pdf(req: ExportRequest):
    if not req.filter_values:
        raise HTTPException(status_code=400, detail="No filter values provided")

    token, site_id = tableau_signin(req.tableau_server, req.site_name, req.pat_name, req.pat_secret)

    try:
        pdf_list = []
        failed = []

        for value in req.filter_values:
            try:
                pdf_bytes = download_view_pdf(
                    req.tableau_server, site_id, req.view_id,
                    token, req.filter_field, value
                )
                pdf_list.append(pdf_bytes)
            except Exception as e:
                failed.append(f"{value}: {str(e)}")

        if not pdf_list:
            raise Exception(f"All exports failed: {failed}")

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
