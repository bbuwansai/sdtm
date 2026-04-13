from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from pathlib import Path
from typing import Any, Optional

import pandas as pd

DONE_VALUES = {"DONE", "APPROVED", "COMPLETE", "COMPLETED", "YES", "Y"}


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
    p = argparse.ArgumentParser(
        description="Rebuild rerun input from Layer 1 clean rows + DONE human-corrected issue rows, preserving row numbers."
    )
    p.add_argument("--clean-rows", required=True, help="Path to ae_rows_clean_for_sdtm.csv from Layer 1")
    p.add_argument("--human-reviewed-rows", required=True, help="Path to reviewed ae_rows_with_issues_raw.csv")
    p.add_argument(
        "--layer1-cmd",
        required=True,
        help=(
            "Command template to rerun Layer 1. Use {source} for rebuilt raw CSV path and {outdir} for output dir. "
            'Example: "python ae_layer1_qc_v7_row_review_stable_rows.py --source {source} --outdir {outdir}"'
        ),
    )
    p.add_argument("--outdir", required=True, help="Directory for pre-SDTM outputs")
    p.add_argument("--review-status-col", default="HUMAN_REVIEW_STATUS")
    p.add_argument("--review-comment-col", default="HUMAN_REVIEW_COMMENT")
    p.add_argument("--issue-summary-col", default="ROW_ISSUES")
    p.add_argument("--row-col", default="L1_SOURCE_ROW_NUMBER")
    p.add_argument("--refreshed-clean-rows", default="ae_rows_clean_for_sdtm.csv")
    p.add_argument("--refreshed-issue-rows", default="ae_rows_with_issues_raw.csv")
    p.add_argument("--refreshed-human-log", default="ae_issue_log_human.csv")
    p.add_argument("--refreshed-sdtm-log", default="ae_issue_log_sdtm_standardisable.csv")
    return p.parse_args()


def validate_inputs(clean_rows_path: Path, human_rows_path: Path) -> None:
    if not clean_rows_path.exists():
        raise FileNotFoundError(f"Clean rows file not found: {clean_rows_path}")
    if not human_rows_path.exists():
        raise FileNotFoundError(f"Reviewed issue rows file not found: {human_rows_path}")


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str)


def validate_row_number_column(df: pd.DataFrame, row_col: str, df_name: str) -> None:
    if row_col not in df.columns:
        raise ValueError(f"{df_name} is missing required column: {row_col}")
    row_nums = pd.to_numeric(df[row_col], errors="coerce")
    if row_nums.isna().any():
        raise ValueError(f"{df_name} contains non-numeric {row_col} values")
    if row_nums.duplicated().any():
        dups = sorted(row_nums[row_nums.duplicated()].astype(int).unique().tolist())
        raise ValueError(f"{df_name} contains duplicate {row_col} values: {dups}")


def sort_by_row_number(df: pd.DataFrame, row_col: str) -> pd.DataFrame:
    out = df.copy()
    out["_SORT_ROW_NUM"] = pd.to_numeric(out[row_col], errors="raise")
    out = out.sort_values("_SORT_ROW_NUM").drop(columns=["_SORT_ROW_NUM"]).reset_index(drop=True)
    return out


def select_done_review_rows(
    reviewed_rows_df: pd.DataFrame,
    review_status_col: str,
    review_comment_col: str,
    issue_summary_col: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if review_status_col not in reviewed_rows_df.columns:
        raise ValueError(f"Reviewed rows file missing required column: {review_status_col}")

    status_norm = reviewed_rows_df[review_status_col].apply(normalize_missing).fillna("").str.upper()
    done_mask = status_norm.isin(DONE_VALUES)

    done_rows = reviewed_rows_df[done_mask].copy()
    pending_rows = reviewed_rows_df[~done_mask].copy()

    helper_cols = [c for c in [review_status_col, review_comment_col, issue_summary_col] if c in done_rows.columns]
    done_rows = done_rows.drop(columns=helper_cols, errors="ignore")

    return done_rows, pending_rows


def ensure_same_raw_columns(clean_rows_df: pd.DataFrame, done_rows_df: pd.DataFrame, row_col: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    clean_cols = list(clean_rows_df.columns)
    done_cols = list(done_rows_df.columns)

    # DONE rows may still contain helper columns if user added extra spreadsheet-like columns.
    allowed_extra = set(done_cols) - set(clean_cols)
    if allowed_extra:
        done_rows_df = done_rows_df[[c for c in done_cols if c in clean_cols]].copy()

    missing_in_done = [c for c in clean_cols if c not in done_rows_df.columns]
    if missing_in_done:
        raise ValueError(f"Reviewed DONE rows are missing raw columns required for rebuild: {missing_in_done}")

    # Reorder to exactly match clean rows.
    done_rows_df = done_rows_df[clean_cols].copy()

    # Ensure clean rows also ordered properly and both preserve row number column.
    if row_col not in clean_rows_df.columns or row_col not in done_rows_df.columns:
        raise ValueError(f"Both clean rows and DONE rows must contain {row_col}")

    return clean_rows_df, done_rows_df


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

    clean_rows_path = Path(args.clean_rows).resolve()
    human_rows_path = Path(args.human_reviewed_rows).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    validate_inputs(clean_rows_path, human_rows_path)

    clean_rows_df = load_csv(clean_rows_path)
    reviewed_rows_df = load_csv(human_rows_path)

    validate_row_number_column(clean_rows_df, args.row_col, "clean rows file")
    validate_row_number_column(reviewed_rows_df, args.row_col, "human-reviewed rows file")

    clean_rows_df = sort_by_row_number(clean_rows_df, args.row_col)
    reviewed_rows_df = sort_by_row_number(reviewed_rows_df, args.row_col)

    done_rows_df, pending_rows_df = select_done_review_rows(
        reviewed_rows_df=reviewed_rows_df,
        review_status_col=args.review_status_col,
        review_comment_col=args.review_comment_col,
        issue_summary_col=args.issue_summary_col,
    )

    if not done_rows_df.empty:
        validate_row_number_column(done_rows_df, args.row_col, "DONE-reviewed rows subset")
        done_rows_df = sort_by_row_number(done_rows_df, args.row_col)

    if not pending_rows_df.empty:
        validate_row_number_column(pending_rows_df, args.row_col, "pending-reviewed rows subset")
        pending_rows_df = sort_by_row_number(pending_rows_df, args.row_col)

    clean_rows_df, done_rows_df = ensure_same_raw_columns(clean_rows_df, done_rows_df, args.row_col)

    clean_row_nums = set(pd.to_numeric(clean_rows_df[args.row_col], errors="raise").astype(int).tolist())
    done_row_nums = set(pd.to_numeric(done_rows_df[args.row_col], errors="raise").astype(int).tolist()) if not done_rows_df.empty else set()
    pending_row_nums = set(pd.to_numeric(pending_rows_df[args.row_col], errors="raise").astype(int).tolist()) if not pending_rows_df.empty else set()

    overlap_clean_done = sorted(clean_row_nums & done_row_nums)
    if overlap_clean_done:
        raise ValueError(
            f"Clean rows file and DONE-reviewed rows overlap on {args.row_col}. "
            f"This means issue rows were not fully removed from the clean file: {overlap_clean_done}"
        )

    expected_total_rows = len(clean_row_nums | done_row_nums | pending_row_nums)
    rebuilt_raw_df = pd.concat([clean_rows_df, done_rows_df], ignore_index=True)
    validate_row_number_column(rebuilt_raw_df, args.row_col, "rebuilt raw dataset before sort")
    rebuilt_raw_df = sort_by_row_number(rebuilt_raw_df, args.row_col)

    rebuilt_row_nums = pd.to_numeric(rebuilt_raw_df[args.row_col], errors="raise").astype(int).tolist()
    if rebuilt_row_nums != sorted(rebuilt_row_nums):
        raise ValueError("Rebuilt raw dataset row numbers are not in ascending order")

    rebuilt_raw_path = outdir / "ae_rebuilt_raw_for_rerun.csv"
    rebuilt_raw_df.to_csv(rebuilt_raw_path, index=False)

    pending_rows_path = outdir / "ae_pending_human_rows.csv"
    pending_rows_df.to_csv(pending_rows_path, index=False)

    # Useful diagnostics before rerun
    diagnostics = {
        "clean_rows_count": int(len(clean_rows_df)),
        "done_reviewed_rows_count": int(len(done_rows_df)),
        "pending_reviewed_rows_count": int(len(pending_rows_df)),
        "rebuilt_rows_count": int(len(rebuilt_raw_df)),
        "expected_total_distinct_row_numbers_seen_in_inputs": int(expected_total_rows),
        "min_row_number_rebuilt": int(min(rebuilt_row_nums)) if rebuilt_row_nums else None,
        "max_row_number_rebuilt": int(max(rebuilt_row_nums)) if rebuilt_row_nums else None,
        "row_numbers_preserved": "Y",
        "row_numbers_sorted_ascending_before_rerun": "Y",
    }
    (outdir / "pre_sdtm_rebuild_diagnostics.json").write_text(json.dumps(diagnostics, indent=2), encoding="utf-8")

    rerun_outdir = outdir / "layer1_rerun_outputs"
    rerun_outdir.mkdir(parents=True, exist_ok=True)
    run_layer1(args.layer1_cmd, rebuilt_raw_path, rerun_outdir)

    refreshed_clean_rows = find_required_output(rerun_outdir, args.refreshed_clean_rows)
    refreshed_issue_rows = find_required_output(rerun_outdir, args.refreshed_issue_rows)
    refreshed_human_log = find_required_output(rerun_outdir, args.refreshed_human_log)
    refreshed_sdtm_log = find_required_output(rerun_outdir, args.refreshed_sdtm_log)

    final_clean_rows = outdir / Path(args.refreshed_clean_rows).name
    final_issue_rows = outdir / Path(args.refreshed_issue_rows).name
    final_human_log = outdir / Path(args.refreshed_human_log).name
    final_sdtm_log = outdir / Path(args.refreshed_sdtm_log).name

    pd.read_csv(refreshed_clean_rows, dtype=str).to_csv(final_clean_rows, index=False)
    pd.read_csv(refreshed_issue_rows, dtype=str).to_csv(final_issue_rows, index=False)
    pd.read_csv(refreshed_human_log, dtype=str).to_csv(final_human_log, index=False)
    pd.read_csv(refreshed_sdtm_log, dtype=str).to_csv(final_sdtm_log, index=False)

    manifest = {
        "input_clean_rows": str(clean_rows_path),
        "input_human_reviewed_rows": str(human_rows_path),
        "rebuilt_raw_for_rerun": str(rebuilt_raw_path),
        "pending_human_rows": str(pending_rows_path),
        "rerun_outdir": str(rerun_outdir),
        "refreshed_clean_rows": str(final_clean_rows),
        "refreshed_issue_rows": str(final_issue_rows),
        "refreshed_human_issue_log": str(final_human_log),
        "refreshed_sdtm_issue_log": str(final_sdtm_log),
        "done_rows_used_for_rerun": int(len(done_rows_df)),
        "pending_rows_kept_aside": int(len(pending_rows_df)),
        "row_numbers_preserved": "Y",
        "row_numbers_sorted_ascending_before_rerun": "Y",
    }
    (outdir / "pre_sdtm_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Pre-SDTM processing complete.")
    print(f"- Clean rows: {len(clean_rows_df)}")
    print(f"- DONE reviewed rows: {len(done_rows_df)}")
    print(f"- Pending reviewed rows: {len(pending_rows_df)}")
    print(f"- Rebuilt raw rows: {len(rebuilt_raw_df)}")
    print(f"- Rebuilt raw dataset for rerun: {rebuilt_raw_path}")
    print(f"- Pending human rows kept aside: {pending_rows_path}")
    print(f"- Refreshed clean rows: {final_clean_rows}")
    print(f"- Refreshed issue rows: {final_issue_rows}")
    print(f"- Refreshed human issue log: {final_human_log}")
    print(f"- Refreshed SDTM-standardizable issue log: {final_sdtm_log}")


if __name__ == "__main__":
    main()
