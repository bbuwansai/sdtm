from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
JOBS_DIR = DATA_DIR / "jobs"
PIPELINES_DIR = BASE_DIR / "pipelines"

for path in [DATA_DIR, UPLOAD_DIR, JOBS_DIR]:
    path.mkdir(parents=True, exist_ok=True)
