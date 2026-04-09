# KlinAI Fullstack Demo

This package contains:

- `frontend/` — Next.js app with a new `/platform` workflow page for upload, domain detection, live process timeline, and downloadable outputs.
- `backend/` — FastAPI backend that creates jobs, auto-detects domains, runs the Layer 1 → Spec → SDTM pipeline, and packages artifacts.

## Frontend

```bash
cd frontend
npm install
npm run dev
```

Set `NEXT_PUBLIC_API_BASE_URL` to your backend URL.

## Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Important notes

- The backend wraps your existing scripts under `backend/pipelines/`.
- I added generated default support assets for missing rule/config files so the orchestration has a starting point.
- For production, replace local job storage with persistent storage and harden security, retention, and auth.
