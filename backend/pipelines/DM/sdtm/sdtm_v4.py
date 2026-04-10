import json
from pathlib import Path
from typing import Optional, Dict, List
import traceback
import pandas as pd

BASE = Path(__file__).resolve().parent
OUTPUT_DIR = BASE / "sdtm_outputs_v4"
RUN_LOG = OUTPUT_DIR / "run_log.txt"

CLEAN_SOURCE_CANDIDATES = [
    "dm_cleaned_output.csv",
    "dm_cleaned_generated_by_code.csv",
    "dm_issue_detected_clean.csv",
    "layer1_cleaned_dm.csv",
]

CONTROLLED_TERMS_CANDIDATES = ["controlled_terminology_demo.csv"]
STUDY_META_CANDIDATES = ["study_metadata_demo.json"]
PROGRAMMING_CONVENTIONS_CANDIDATES = ["programming_conventions_demo.json"]
SPONSOR_RULES_CANDIDATES = ["demo_sponsor_rules_dm.json"]

LAYER1_HUMAN_CANDIDATES = ["dm_human_review_issues.csv"]
LAYER1_SDTM_FIXABLE_CANDIDATES = ["dm_sdtm_standardizable_issues.csv"]

FINAL_COLUMNS = [
    "STUDYID", "DOMAIN", "USUBJID", "SUBJID", "SITEID",
    "RFSTDTC", "BRTHDTC", "AGE", "AGEU", "SEX",
    "RACE", "ETHNIC", "COUNTRY", "ARM", "ACTARM",
    "DTHFL", "DTHDTC",
]


def init_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_LOG, "w", encoding="utf-8") as f:
        f.write(f"BASE={BASE}\n")
        f.write(f"OUTPUT_DIR={OUTPUT_DIR}\n")


def log(msg: str):
    print(msg)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUN_LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def first_existing(base: Path, candidates: List[str], required: bool = True) -> Optional[Path]:
    for c in candidates:
        p = base / c
        if p.exists():
            return p
    if required:
        raise FileNotFoundError(f"None of these files were found under {base}: {candidates}")
    return None


def load_inputs():
    clean_source_path = first_existing(BASE, CLEAN_SOURCE_CANDIDATES)
    ct_path = first_existing(BASE, CONTROLLED_TERMS_CANDIDATES)
    meta_path = first_existing(BASE, STUDY_META_CANDIDATES)
    prog_path = first_existing(BASE, PROGRAMMING_CONVENTIONS_CANDIDATES)
    sponsor_rules_path = first_existing(BASE, SPONSOR_RULES_CANDIDATES)
    human_path = first_existing(BASE, LAYER1_HUMAN_CANDIDATES, required=False)
    sdtm_fixable_path = first_existing(BASE, LAYER1_SDTM_FIXABLE_CANDIDATES, required=False)

    log(f"Using clean source: {clean_source_path}")
    log(f"Using CT: {ct_path}")
    log(f"Using study metadata: {meta_path}")
    log(f"Using programming conventions: {prog_path}")
    log(f"Using sponsor rules: {sponsor_rules_path}")
    log(f"Using human issues file: {human_path}")
    log(f"Using sdtm-fixable issues file: {sdtm_fixable_path}")

    source_df = pd.read_csv(clean_source_path, dtype=str)
    ct_df = pd.read_csv(ct_path, dtype=str)
    study_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    prog_conv = json.loads(prog_path.read_text(encoding="utf-8"))
    sponsor_rules = json.loads(sponsor_rules_path.read_text(encoding="utf-8"))

    human_df = pd.read_csv(human_path, dtype=str) if human_path else pd.DataFrame()
    sdtm_fixable_df = pd.read_csv(sdtm_fixable_path, dtype=str) if sdtm_fixable_path else pd.DataFrame()

    return source_df, ct_df, study_meta, prog_conv, sponsor_rules, human_df, sdtm_fixable_df


def clean_value(val: Optional[str], missing_values: List[str]) -> Optional[str]:
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "":
        return None
    if s.upper() in {str(x).upper() for x in missing_values}:
        return None
    if s.upper() in {"NAN", "NONE"}:
        return None
    return s


def normalize_df(df: pd.DataFrame, prog_conv: dict) -> pd.DataFrame:
    out = df.copy()
    missing_values = prog_conv.get("missing_values", ["", "NA", "NULL"])
    for col in out.columns:
        out[col] = out[col].apply(lambda x: clean_value(x, missing_values))
    return out


def normalize_id(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def normalize_age_token(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    import re
    s = str(val).strip()
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)", s)
    if not m:
        return None
    try:
        f = float(m.group(1))
        return str(int(f)) if f.is_integer() else str(f)
    except Exception:
        return None


def to_iso_partial_date(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    import re
    s = str(val).strip()
    if re.fullmatch(r"\d{4}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    return None


def is_full_date(val: Optional[str]) -> bool:
    return isinstance(val, str) and len(val) == 10


def calculate_age_years(brthdtc: str, rfstdtc: str) -> Optional[str]:
    try:
        b = pd.to_datetime(brthdtc)
        r = pd.to_datetime(rfstdtc)
        years = r.year - b.year - ((r.month, r.day) < (b.month, b.day))
        return str(years)
    except Exception:
        return None


def get_row_value(row: pd.Series, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in row and row.get(c) not in [None, ""]:
            return row.get(c)
    return None


def derive_usubjid(row: dict) -> Optional[str]:
    if row.get("USUBJID"):
        return row.get("USUBJID")
    if row.get("STUDYID") and row.get("SITEID") and row.get("SUBJID"):
        return f"{row['STUDYID']}-{row['SITEID']}-{row['SUBJID']}"
    return None


def rows_with_human_issues(human_df: pd.DataFrame) -> set:
    if human_df.empty or "row_num" not in human_df.columns:
        return set()
    out = set()
    for x in human_df["row_num"].dropna().astype(str):
        try:
            out.add(int(float(x)))
        except Exception:
            pass
    return out


def build_dm(source_df: pd.DataFrame, study_meta: dict, sponsor_rules: dict, human_df: pd.DataFrame):
    final_rows = []
    exception_rows = []
    qc_rows = []

    human_rows = rows_with_human_issues(human_df)
    log(f"Human review rows detected: {len(human_rows)}")

    required_for_final = set(sponsor_rules.get("required_for_demo_final", ["STUDYID", "DOMAIN", "USUBJID", "SUBJID", "SITEID", "SEX"]))

    for i, src_row in source_df.iterrows():
        rownum = i + 1
        row_issues = []
        hard_fail = False

        out = {c: get_row_value(src_row, c) for c in FINAL_COLUMNS}
        out["STUDYID"] = get_row_value(src_row, "STUDYID", "PROTOCOL_ID") or study_meta.get("study_id")
        out["DOMAIN"] = "DM"
        out["SUBJID"] = normalize_id(get_row_value(src_row, "SUBJID", "SUBJECT_NUMBER"))
        out["SITEID"] = normalize_id(get_row_value(src_row, "SITEID", "SITE_NUMBER"))
        out["RFSTDTC"] = to_iso_partial_date(get_row_value(src_row, "RFSTDTC", "REF_START_DT"))
        out["BRTHDTC"] = to_iso_partial_date(get_row_value(src_row, "BRTHDTC", "DATE_OF_BIRTH"))
        out["DTHDTC"] = to_iso_partial_date(get_row_value(src_row, "DTHDTC", "DEATH_DATE"))

        collected_age = normalize_age_token(get_row_value(src_row, "AGE", "AGE_AT_REF"))
        if is_full_date(out["BRTHDTC"]) and is_full_date(out["RFSTDTC"]):
            out["AGE"] = calculate_age_years(out["BRTHDTC"], out["RFSTDTC"]) or collected_age
        else:
            out["AGE"] = collected_age

        out["AGEU"] = get_row_value(src_row, "AGEU", "AGE_UNITS") or ("YEARS" if out["AGE"] is not None else None)
        out["SEX"] = get_row_value(src_row, "SEX", "SEX_AT_BIRTH")
        out["RACE"] = get_row_value(src_row, "RACE", "RACE_CAT")
        out["ETHNIC"] = get_row_value(src_row, "ETHNIC", "ETHNIC_GRP")
        out["COUNTRY"] = get_row_value(src_row, "COUNTRY", "COUNTRY_CODE")
        out["ARM"] = get_row_value(src_row, "ARM", "PLANNED_TRT_ARM")
        out["ACTARM"] = get_row_value(src_row, "ACTARM", "ACTUAL_TRT_ARM")
        out["DTHFL"] = get_row_value(src_row, "DTHFL", "DEATH_IND")
        out["USUBJID"] = derive_usubjid(out)

        for req in required_for_final:
            if not out.get(req):
                row_issues.append(f"{req} missing")
                hard_fail = True

        if rownum in human_rows:
            row_issues.append("Row has HUMAN_REVIEW issues from Layer 1; excluded from final SDTM until resolved")
            hard_fail = True

        exception_record = {c: out.get(c) for c in FINAL_COLUMNS}
        exception_record["SOURCE_ROW_NUMBER"] = rownum
        exception_record["ROW_ISSUES"] = " | ".join(row_issues)

        qc_rows.append({
            "SOURCE_ROW_NUMBER": rownum,
            "USUBJID": out.get("USUBJID"),
            "ISSUE_COUNT": len(row_issues),
            "ISSUES": " | ".join(row_issues),
            "DISPOSITION": "EXCEPTION" if hard_fail else ("PASS_WITH_WARNINGS" if row_issues else "PASS"),
        })

        if hard_fail:
            exception_rows.append(exception_record)
        else:
            final_rows.append({c: out.get(c) for c in FINAL_COLUMNS})

    return pd.DataFrame(final_rows), pd.DataFrame(exception_rows), pd.DataFrame(qc_rows)


def write_outputs(final_df, exceptions_df, qc_df):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    final_df = final_df.reindex(columns=FINAL_COLUMNS)
    final_df.to_csv(OUTPUT_DIR / "dm_sdtm_final_v7.csv", index=False)
    exceptions_df.to_csv(OUTPUT_DIR / "dm_sdtm_exceptions_v7.csv", index=False)
    qc_df.to_csv(OUTPUT_DIR / "dm_sdtm_qc_report_v7.csv", index=False)
    log(f"Wrote final rows: {len(final_df)}")
    log(f"Wrote exception rows: {len(exceptions_df)}")
    log(f"Wrote QC rows: {len(qc_df)}")


def main():
    init_output_dir()
    try:
        source_df, ct_df, study_meta, prog_conv, sponsor_rules, human_df, sdtm_fixable_df = load_inputs()
        source_df = normalize_df(source_df, prog_conv)
        final_df, exceptions_df, qc_df = build_dm(source_df, study_meta, sponsor_rules, human_df)
        write_outputs(final_df, exceptions_df, qc_df)
        log("Completed successfully")
    except Exception as e:
        log("RUN FAILED")
        log(str(e))
        log(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
