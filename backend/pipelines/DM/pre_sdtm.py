
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Optional

import pandas as pd

DONE_VALUES = {"DONE", "APPROVED", "COMPLETE", "COMPLETED", "YES", "Y"}


@dataclass
class AppliedChange:
    row_num: int
    field: str
    old_value: Optional[str]
    new_value: Optional[str]
    rule_id: Optional[str]
    rule_description: Optional[str]
    review_status: Optional[str]
    review_comment: Optional[str]
    applied: str
    reason: str


def normalize_missing(value: Any) -> Optional[str]:
    if value is None:
        return None
    if pd.isna(value):
        return None
    s = str(value).strip()
    if s == "":
        return None
    if s.upper() in {"NAN", "NONE", "NULL"}:
        return None
    return s


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Apply human-reviewed corrections and rerun Layer 1.")
    p.add_argument("--cleaned", required=True, help="Path to cleaned output CSV from Layer 1")
    p.add_argument("--human-reviewed", required=True, help="Path to reviewed human issue CSV")
    p.add_argument(
        "--layer1-cmd",
        required=True,
        help=(
            "Command template to rerun Layer 1. Use {source} for corrected CSV path and {outdir} for output dir. "
            'Example: "python main2.py --source {source} --outdir {outdir}"'
        ),
    )
    p.add_argument("--outdir", required=True, help="Directory for pre-SDTM outputs")
    p.add_argument("--review-status-col", default="review_status")
    p.add_argument("--review-value-col", default="human_reviewed_value")
    p.add_argument("--review-comment-col", default="review_comment")
    p.add_argument("--row-col", default="row_num")
    p.add_argument("--field-col", default="field")
    p.add_argument("--refreshed-cleaned", default="dm_cleaned_output.csv")
    p.add_argument("--refreshed-human", default="dm_human_review_issues.csv")
    p.add_argument("--refreshed-sdtm", default="dm_sdtm_standardizable_issues.csv")
    p.add_argument("--strict-field-check", choices=["Y", "N"], default="Y")
    return p.parse_args()


def validate_inputs(cleaned_path: Path, human_path: Path) -> None:
    if not cleaned_path.exists():
        raise FileNotFoundError(f"Cleaned file not found: {cleaned_path}")
    if not human_path.exists():
        raise FileNotFoundError(f"Reviewed human file not found: {human_path}")


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str)


def should_apply(status: Optional[str], reviewed_value: Optional[str]) -> bool:
    if normalize_missing(status) is None:
        return False
    if normalize_missing(reviewed_value) is None:
        return False
    return normalize_missing(status).upper() in DONE_VALUES


def apply_reviewed_changes(
    cleaned_df: pd.DataFrame,
    reviewed_df: pd.DataFrame,
    row_col: str,
    field_col: str,
    status_col: str,
    value_col: str,
    comment_col: str,
    strict_field_check: bool,
) -> tuple[pd.DataFrame, list[AppliedChange]]:
    updated = cleaned_df.copy()
    changes: list[AppliedChange] = []

    required_cols = {row_col, field_col, status_col, value_col}
    missing = [c for c in required_cols if c not in reviewed_df.columns]
    if missing:
        raise ValueError(f"Reviewed human file missing required columns: {missing}")

    for _, issue in reviewed_df.iterrows():
        row_raw = issue.get(row_col)
        field = normalize_missing(issue.get(field_col))
        status = normalize_missing(issue.get(status_col))
        reviewed_value = normalize_missing(issue.get(value_col))
        review_comment = normalize_missing(issue.get(comment_col)) if comment_col in reviewed_df.columns else None
        rule_id = normalize_missing(issue.get("rule_id")) if "rule_id" in reviewed_df.columns else None
        rule_description = normalize_missing(issue.get("rule_description")) if "rule_description" in reviewed_df.columns else None

        try:
            row_num = int(float(str(row_raw)))
        except Exception:
            changes.append(
                AppliedChange(
                    row_num=-1,
                    field=field or "",
                    old_value=None,
                    new_value=reviewed_value,
                    rule_id=rule_id,
                    rule_description=rule_description,
                    review_status=status,
                    review_comment=review_comment,
                    applied="N",
                    reason=f"Invalid row number: {row_raw}",
                )
            )
            continue

        if not should_apply(status, reviewed_value):
            changes.append(
                AppliedChange(
                    row_num=row_num,
                    field=field or "",
                    old_value=None,
                    new_value=reviewed_value,
                    rule_id=rule_id,
                    rule_description=rule_description,
                    review_status=status,
                    review_comment=review_comment,
                    applied="N",
                    reason="Skipped because review_status is not DONE/APPROVED or reviewed value is blank",
                )
            )
            continue

        if field is None:
            changes.append(
                AppliedChange(
                    row_num=row_num,
                    field="",
                    old_value=None,
                    new_value=reviewed_value,
                    rule_id=rule_id,
                    rule_description=rule_description,
                    review_status=status,
                    review_comment=review_comment,
                    applied="N",
                    reason="Missing field name",
                )
            )
            continue

        zero_idx = row_num - 1
        if zero_idx < 0 or zero_idx >= len(updated):
            changes.append(
                AppliedChange(
                    row_num=row_num,
                    field=field,
                    old_value=None,
                    new_value=reviewed_value,
                    rule_id=rule_id,
                    rule_description=rule_description,
                    review_status=status,
                    review_comment=review_comment,
                    applied="N",
                    reason="Row number outside cleaned dataset range",
                )
            )
            continue

        if field not in updated.columns:
            if strict_field_check:
                changes.append(
                    AppliedChange(
                        row_num=row_num,
                        field=field,
                        old_value=None,
                        new_value=reviewed_value,
                        rule_id=rule_id,
                        rule_description=rule_description,
                        review_status=status,
                        review_comment=review_comment,
                        applied="N",
                        reason="Target field not present in cleaned dataset",
                    )
                )
                continue
            updated[field] = pd.NA

        old_value = normalize_missing(updated.at[zero_idx, field])
        updated.at[zero_idx, field] = reviewed_value

        changes.append(
            AppliedChange(
                row_num=row_num,
                field=field,
                old_value=old_value,
                new_value=reviewed_value,
                rule_id=rule_id,
                rule_description=rule_description,
                review_status=status,
                review_comment=review_comment,
                applied="Y",
                reason="Applied reviewed correction",
            )
        )

    return updated, changes


def run_layer1(layer1_cmd_template: str, corrected_source: Path, outdir: Path) -> None:
    cmd = layer1_cmd_template.format(source=str(corrected_source), outdir=str(outdir))
    result = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            "Layer 1 rerun failed.\n"
            f"Command: {cmd}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )


def find_required_output(base: Path, filename: str) -> Path:
    exact = base / filename
    if exact.exists():
        return exact
    matches = list(base.rglob(filename))
    if matches:
        return matches[0]
    raise FileNotFoundError(f"Expected refreshed output not found: {filename} under {base}")


def main() -> None:
    args = parse_args()

    cleaned_path = Path(args.cleaned).resolve()
    human_path = Path(args.human_reviewed).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    validate_inputs(cleaned_path, human_path)

    cleaned_df = load_csv(cleaned_path)
    reviewed_df = load_csv(human_path)

    corrected_df, changes = apply_reviewed_changes(
        cleaned_df=cleaned_df,
        reviewed_df=reviewed_df,
        row_col=args.row_col,
        field_col=args.field_col,
        status_col=args.review_status_col,
        value_col=args.review_value_col,
        comment_col=args.review_comment_col,
        strict_field_check=args.strict_field_check == "Y",
    )

    corrected_cleaned_path = outdir / f"{cleaned_path.stem}_human_applied.csv"
    corrected_df.to_csv(corrected_cleaned_path, index=False)

    audit_df = pd.DataFrame([asdict(c) for c in changes])
    audit_path = outdir / "human_review_apply_audit.csv"
    audit_df.to_csv(audit_path, index=False)

    rerun_outdir = outdir / "layer1_rerun_outputs"
    rerun_outdir.mkdir(parents=True, exist_ok=True)
    run_layer1(args.layer1_cmd, corrected_cleaned_path, rerun_outdir)

    refreshed_cleaned = find_required_output(rerun_outdir, args.refreshed_cleaned)
    refreshed_human = find_required_output(rerun_outdir, args.refreshed_human)
    refreshed_sdtm = find_required_output(rerun_outdir, args.refreshed_sdtm)

    final_cleaned = outdir / Path(args.refreshed_cleaned).name
    final_human = outdir / Path(args.refreshed_human).name
    final_sdtm = outdir / Path(args.refreshed_sdtm).name

    pd.read_csv(refreshed_cleaned, dtype=str).to_csv(final_cleaned, index=False)
    pd.read_csv(refreshed_human, dtype=str).to_csv(final_human, index=False)
    pd.read_csv(refreshed_sdtm, dtype=str).to_csv(final_sdtm, index=False)

    manifest = {
        "corrected_cleaned_source": str(corrected_cleaned_path),
        "applied_audit": str(audit_path),
        "rerun_outdir": str(rerun_outdir),
        "refreshed_cleaned_output": str(final_cleaned),
        "refreshed_human_issue_log": str(final_human),
        "refreshed_sdtm_issue_log": str(final_sdtm),
        "applied_changes": int((audit_df["applied"] == "Y").sum()) if not audit_df.empty else 0,
        "skipped_changes": int((audit_df["applied"] != "Y").sum()) if not audit_df.empty else 0,
    }
    (outdir / "pre_sdtm_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Pre-SDTM processing complete.")
    print(f"- Corrected cleaned dataset: {corrected_cleaned_path}")
    print(f"- Audit log: {audit_path}")
    print(f"- Refreshed cleaned output: {final_cleaned}")
    print(f"- Refreshed human issue log: {final_human}")
    print(f"- Refreshed SDTM-standardizable issue log: {final_sdtm}")


if __name__ == "__main__":
    main()
