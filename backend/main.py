from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from app.config import JOBS_DIR
from app.services.domain_detector import detect_domain
from app.services.job_store import JobStore
from app.services.pipeline_runner import create_job

app = FastAPI(title="KlinAI Demo Backend", version="0.1.0")
store = JobStore()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/api/detect-domain")
async def detect_domain_api(file: UploadFile = File(...)):
    raw = await file.read()
    return detect_domain(raw, file.filename or "upload.csv")


@app.post("/api/jobs")
async def create_job_api(
    file: UploadFile = File(...),
    domain: str = Form("AUTO"),
    phase: str = Form("layer1_spec"),
    reviewed_human_file: UploadFile | None = File(None),
):
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    reviewed_bytes = None
    reviewed_name = None
    if reviewed_human_file is not None:
        reviewed_bytes = await reviewed_human_file.read()
        reviewed_name = reviewed_human_file.filename or "reviewed_human_issues.csv"

    job_id = create_job(
        filename=file.filename or "upload.csv",
        file_bytes=raw,
        selected_domain=domain,
        phase=phase,
        reviewed_human_filename=reviewed_name,
        reviewed_human_bytes=reviewed_bytes,
    )
    return {"job_id": job_id}

@app.post("/api/jobs/{job_id}/run-sdtm")
async def run_sdtm_api(
    job_id: str,
    reviewed_human_file: UploadFile = File(...),
):
    reviewed_bytes = await reviewed_human_file.read()
    if not reviewed_bytes:
        raise HTTPException(status_code=400, detail="Reviewed human file is empty")

    reviewed_path = save_reviewed_human_file(job_id, reviewed_human_file.filename or "reviewed_human_issues.csv", reviewed_bytes)
    start_sdtm_phase(job_id, reviewed_path)
    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    try:
        return store.load(job_id).to_dict()
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Job not found")


@app.get("/api/jobs/{job_id}/download/{filename}")
def download_artifact(job_id: str, filename: str):
    path = JOBS_DIR / job_id / "artifacts" / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Artifact not found")
    return FileResponse(path, filename=filename)
