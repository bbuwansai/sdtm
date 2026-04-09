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
async def create_job_api(file: UploadFile = File(...), domain: str = Form("AUTO")):
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    job_id = create_job(file.filename or "upload.csv", raw, domain)
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
