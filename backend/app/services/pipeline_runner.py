from __future__ import annotations

import shutil
import subprocess
import threading
import uuid
import zipfile
from pathlib import Path
from typing import Dict, List

import pandas as pd

from app.config import JOBS_DIR, PIPELINES_DIR
from app.services.default_assets import ensure_dm_support_assets
from app.services.domain_detector import detect_domain
from app.services.job_store import JobStore

store = JobStore()


def copy_pipeline_template(domain: str, job_dir: Path) -> Path:
    template_dir = PIPELINES_DIR / domain
    target = job_dir / "workspace" / domain
    shutil.copytree(template_dir, target, dirs_exist_ok=True)
    return target


def run_command(command: List[str], cwd: Path, job_id: str, step: str) -> None:
    store.append(job_id, f"Starting {step}.", step=step)
    result = subprocess.run(command, cwd=str(cwd), capture_output=True, text=True)
    if result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            store.append(job_id, line[:500], level="info", step=step)
    if result.returncode != 0:
        if result.stderr.strip():
            for line in result.stderr.strip().splitlines():
                store.append(job_id, line[:500], level="error", step=step)
        raise RuntimeError(f"{step} failed")
    store.append(job_id, f"Completed {step}.", level="success", step=step)


def collect_files(paths: List[Path], zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in paths:
            if path.is_dir():
                for file in path.rglob("*"):
                    if file.is_file() and ".DS_Store" not in file.name and "__MACOSX" not in str(file):
                        zf.write(file, arcname=file.relative_to(path.parent))
            elif path.is_file():
                zf.write(path, arcname=path.name)


def count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return len(pd.read_csv(path))
    except Exception:
        return 0


def find_first_csv(directory: Path) -> Path | None:
    for file in directory.glob("*.csv"):
        return file
    return None


def summarise_csv(path: Path) -> Dict[str, str]:
    if not path or not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
        return {"rows": str(len(df)), "columns": str(len(df.columns))}
    except Exception:
        return {}


def prepare_domain_inputs(domain_dir: Path, domain: str, upload_path: Path) -> None:
    store_name = "raw_input.csv"
    if domain == "DM":
        shutil.copy2(upload_path, domain_dir / "Layer1" / store_name)
        shutil.copy2(upload_path, domain_dir / "Layer1" / "dm_source_50_rows.csv")
        shutil.copy2(upload_path, domain_dir / "Spec" / store_name)
        shutil.copy2(upload_path, domain_dir / "Spec" / "dm_raw_crf_style_50_rows.csv")
        ensure_dm_support_assets(domain_dir / "sdtm")
    elif domain == "VS":
        shutil.copy2(upload_path, domain_dir / "Layer1" / store_name)
        shutil.copy2(upload_path, domain_dir / "Layer1" / "vs_raw_crf_style_demo.csv")
        shutil.copy2(upload_path, domain_dir / "Spec" / store_name)
        shutil.copy2(upload_path, domain_dir / "Spec" / "vs_raw_crf_style_demo.csv")
        shutil.copy2(upload_path, domain_dir / "sdtm" / store_name)
        shutil.copy2(upload_path, domain_dir / "sdtm" / "vs_raw_crf_style_demo.csv")
    elif domain == "LB":
        shutil.copy2(upload_path, domain_dir / "Layer1" / store_name)
        shutil.copy2(upload_path, domain_dir / "Layer1" / "lb_raw.csv")
        shutil.copy2(upload_path, domain_dir / "Spec" / store_name)
        shutil.copy2(upload_path, domain_dir / "Spec" / "lb_raw.csv")
    elif domain == "AE":
        shutil.copy2(upload_path, domain_dir / "Layer1" / store_name)
        shutil.copy2(upload_path, domain_dir / "Layer1" / "ae_raw.csv")
        shutil.copy2(upload_path, domain_dir / "Spec" / store_name)
        shutil.copy2(upload_path, domain_dir / "Spec" / "ae_raw.csv")


def add_issue_summary(job_id: str, issue_file: Path, duplicate_file: Path | None = None, human_file: Path | None = None) -> Dict[str, str]:
    metrics: Dict[str, str] = {}
    issues_count = count_rows(issue_file)
    if issues_count:
        metrics["issues_found"] = str(issues_count)
        store.append(job_id, f"Found {issues_count} issues during quality checks.", level="warning", step="Layer 1 QC")
    if duplicate_file and duplicate_file.exists():
        dup_count = count_rows(duplicate_file)
        metrics["duplicates_found"] = str(dup_count)
        store.append(job_id, f"Identified {dup_count} duplicate or duplicate-candidate records.", level="info", step="Layer 1 QC")
    if human_file and human_file.exists():
        human_count = count_rows(human_file)
        metrics["human_review_rows"] = str(human_count)
        store.append(job_id, f"Routed {human_count} rows to the human review queue while automation continues for the demo.", level="info", step="Layer 1 QC")
    return metrics


def execute_domain_pipeline(job_id: str, domain: str, upload_path: Path) -> None:
    job_dir = JOBS_DIR / job_id
    domain_dir = copy_pipeline_template(domain, job_dir)
    prepare_domain_inputs(domain_dir, domain, upload_path)

    store.append(job_id, f"Classified uploaded dataset as {domain}. Routing to the {domain} pipeline.", level="success", step="Domain detection")
    store.append(job_id, "Beginning automated handoff: raw data → Layer 1 QC → spec generation → SDTM.", step="Orchestration")

    metrics: Dict[str, str] = {}
    if domain == "DM":
        store.append(job_id, "Processing QC for the raw DM data.", step="Layer 1 QC")
        run_command(["python", "main2.py"], domain_dir / "Layer1", job_id, "Layer 1 QC")
        metrics.update(add_issue_summary(job_id, domain_dir / "Layer1" / "dm_issue_log.csv", human_file=domain_dir / "Layer1" / "dm_human_review.csv"))

        store.append(job_id, "Generating the spec package directly from the raw DM data.", step="Spec generation")
        run_command(["python", "spec_final.py"], domain_dir / "Spec", job_id, "Spec generation")

        store.append(job_id, "Preparing SDTM inputs using the raw data, Layer 1 outputs, and generated spec artifacts.", step="SDTM generation")
        shutil.copy2(domain_dir / "Layer1" / "dm_cleaned_output.csv", domain_dir / "sdtm" / "dm_cleaned_output.csv")
        run_command(["python", "sdtm_v4.py"], domain_dir / "sdtm", job_id, "SDTM generation")
        layer1_out = domain_dir / "Layer1"
        spec_out = domain_dir / "Spec" / "ai_dm_spec_outputs_v5"
        sdtm_out = domain_dir / "sdtm" / "sdtm_outputs_v4"
    elif domain == "VS":
        store.append(job_id, "Processing QC for the raw VS data.", step="Layer 1 QC")
        run_command(["python", "layer1_v4.py"], domain_dir / "Layer1", job_id, "Layer 1 QC")
        metrics.update(add_issue_summary(job_id, domain_dir / "Layer1" / "vs_layer1_outputs_v4" / "vs_issue_log.csv", duplicate_file=domain_dir / "Layer1" / "vs_layer1_outputs_v4" / "vs_duplicates.csv"))

        store.append(job_id, "Generating the spec package directly from the raw VS data.", step="Spec generation")
        run_command(["python", "spec_v3.py"], domain_dir / "Spec", job_id, "Spec generation")

        store.append(job_id, "Preparing SDTM inputs using the raw data, Layer 1 outputs, and generated spec artifacts.", step="SDTM generation")
        run_command(["python", "sdtm_v4.py"], domain_dir / "sdtm", job_id, "SDTM generation")
        layer1_out = domain_dir / "Layer1" / "vs_layer1_outputs_v4"
        spec_out = domain_dir / "Spec" / "vs_spec_outputs_v3"
        sdtm_out = domain_dir / "sdtm" / "vs_sdtm_outputs_v4"
    elif domain == "LB":
        store.append(job_id, "Processing QC for the raw LB data.", step="Layer 1 QC")
        run_command(["python", "layer1_v6.py", "--source", str(domain_dir / "Layer1" / "lb_raw.csv"), "--outdir", str(domain_dir / "Layer1" / "lb_layer1_outputs_v5")], domain_dir / "Layer1", job_id, "Layer 1 QC")
        metrics.update(add_issue_summary(job_id, domain_dir / "Layer1" / "lb_layer1_outputs_v5" / "lb_issue_log_all.csv", duplicate_file=domain_dir / "Layer1" / "lb_layer1_outputs_v5" / "lb_duplicates.csv", human_file=domain_dir / "Layer1" / "lb_layer1_outputs_v5" / "lb_human_review.csv"))

        store.append(job_id, "Generating the spec package directly from the raw LB data.", step="Spec generation")
        run_command(["python", "spec4.py"], domain_dir / "Spec", job_id, "Spec generation")

        store.append(job_id, "Preparing SDTM inputs using the raw data, Layer 1 outputs, and generated spec artifacts.", step="SDTM generation")
        project_dir = job_dir / "workspace" / "lb_project"
        project_dir.mkdir(parents=True, exist_ok=True)
        for file in (domain_dir / "Layer1" / "lb_layer1_outputs_v5").glob("*.csv"):
            shutil.copy2(file, project_dir / file.name)
        for file in (domain_dir / "Spec" / "lb_spec_outputs_v4").glob("*.csv"):
            shutil.copy2(file, project_dir / file.name)
        shutil.copy2(upload_path, project_dir / "lb_raw.csv")
        run_command(["python", str(domain_dir / "sdtm" / "sdtm1.py"), "--project-folder", str(project_dir), "--outdir", str(project_dir / "lb_sdtm_output_v2")], project_dir, job_id, "SDTM generation")
        layer1_out = domain_dir / "Layer1" / "lb_layer1_outputs_v5"
        spec_out = domain_dir / "Spec" / "lb_spec_outputs_v4"
        sdtm_out = project_dir / "lb_sdtm_output_v2"
    else:
        store.append(job_id, "Processing QC for the raw AE data.", step="Layer 1 QC")
        run_command(["python", "layer1.py", "--source", str(domain_dir / "Layer1" / "ae_raw.csv"), "--rules", str(domain_dir / "Layer1" / "ae_layer1_rules_v6.json"), "--outdir", str(domain_dir / "Layer1" / "ae_layer1_outputs_v6")], domain_dir / "Layer1", job_id, "Layer 1 QC")
        metrics.update(add_issue_summary(job_id, domain_dir / "Layer1" / "ae_layer1_outputs_v6" / "ae_issue_log_all.csv", duplicate_file=domain_dir / "Layer1" / "ae_layer1_outputs_v6" / "ae_duplicates.csv", human_file=domain_dir / "Layer1" / "ae_layer1_outputs_v6" / "ae_human_review.csv"))

        store.append(job_id, "Generating the spec package directly from the raw AE data.", step="Spec generation")
        run_command(["python", "spec2.py"], domain_dir / "Spec", job_id, "Spec generation")

        store.append(job_id, "Preparing SDTM inputs using the raw data, Layer 1 outputs, and generated spec artifacts.", step="SDTM generation")
        project_dir = job_dir / "workspace" / "ae_project"
        project_dir.mkdir(parents=True, exist_ok=True)
        for file in (domain_dir / "Layer1" / "ae_layer1_outputs_v6").glob("*.csv"):
            shutil.copy2(file, project_dir / file.name)
        for file in (domain_dir / "Spec" / "ae_spec_outputs_v2").glob("*.csv"):
            shutil.copy2(file, project_dir / file.name)
        shutil.copy2(upload_path, project_dir / "ae_raw.csv")
        run_command(["python", str(domain_dir / "Sdtm" / "sdtm2.py"), "--project-folder", str(project_dir), "--outdir", str(project_dir / "ae_sdtm_output_v2")], project_dir, job_id, "SDTM generation")
        layer1_out = domain_dir / "Layer1" / "ae_layer1_outputs_v6"
        spec_out = domain_dir / "Spec" / "ae_spec_outputs_v2"
        sdtm_out = project_dir / "ae_sdtm_output_v2"

    zips_dir = job_dir / "artifacts"
    zips_dir.mkdir(parents=True, exist_ok=True)
    layer1_zip = zips_dir / f"{domain.lower()}_layer1_output.zip"
    spec_zip = zips_dir / f"{domain.lower()}_spec_output.zip"
    sdtm_zip = zips_dir / f"{domain.lower()}_sdtm_output.zip"
    complete_zip = zips_dir / f"{domain.lower()}_complete_package.zip"
    collect_files([layer1_out], layer1_zip)
    collect_files([spec_out], spec_zip)
    collect_files([sdtm_out], sdtm_zip)
    collect_files([layer1_out, spec_out, sdtm_out], complete_zip)

    final_csv = find_first_csv(sdtm_out)
    metrics.update(summarise_csv(final_csv) if final_csv else {})
    metrics["domain"] = domain
    store.patch(job_id, status="completed", current_step="Completed", artifacts={
        "Layer 1 output": f"/api/jobs/{job_id}/download/{layer1_zip.name}",
        "Spec package": f"/api/jobs/{job_id}/download/{spec_zip.name}",
        "SDTM output": f"/api/jobs/{job_id}/download/{sdtm_zip.name}",
        "Complete package": f"/api/jobs/{job_id}/download/{complete_zip.name}",
    }, metrics=metrics)
    store.append(job_id, "Automation complete. For the demo, human-review rows are surfaced but the pipeline continues automatically.", level="success", step="Completed")



def process_job(job_id: str, upload_path: Path, forced_domain: str = "AUTO") -> None:
    try:
        store.patch(job_id, status="running")
        payload = detect_domain(upload_path.read_bytes(), upload_path.name)
        detected = payload["domain"]
        domain = detected if forced_domain == "AUTO" else forced_domain
        store.patch(job_id, domain=domain, domain_confidence=payload["confidence"])
        matched = ", ".join(payload["matched_columns"]) or "no high-signal columns"
        store.append(job_id, f"Detected domain {detected} using matched columns: {matched}.", level="info", step="Domain detection")
        if forced_domain != "AUTO" and forced_domain != detected:
            store.append(job_id, f"Using manual override domain {forced_domain} for this run.", level="warning", step="Domain detection")
        execute_domain_pipeline(job_id, domain, upload_path)
    except Exception as exc:
        store.patch(job_id, status="failed", error=str(exc))
        store.append(job_id, f"Run failed: {exc}", level="error")



def create_job(filename: str, file_bytes: bytes, selected_domain: str = "AUTO") -> str:
    job_id = str(uuid.uuid4())
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    upload_path = job_dir / filename
    upload_path.write_bytes(file_bytes)
    store.create(job_id=job_id, filename=filename, domain=None if selected_domain == "AUTO" else selected_domain)
    store.append(job_id, "Upload received.", step="Upload")
    thread = threading.Thread(target=process_job, args=(job_id, upload_path, selected_domain), daemon=True)
    thread.start()
    return job_id
