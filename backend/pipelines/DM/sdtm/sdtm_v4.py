import json
from pathlib import Path
from typing import Optional, Dict, List, Set

import pandas as pd

BASE = Path(__file__).resolve().parent
OUTPUT_DIR = BASE / "sdtm_outputs_v5"

CLEAN_SOURCE_CANDIDATES = [
    "dm_cleaned_output.csv",
    "dm_cleaned_generated_by_code.csv",
    "dm_issue_detected_clean.csv",
    "layer1_cleaned_dm.csv",
]
HUMAN_ISSUES_CANDIDATES = ["dm_human_review_issues.csv"]
SDTM_FIXABLE_ISSUES_CANDIDATES = ["dm_sdtm_standardizable_issues.csv"]
CONTROLLED_TERMS_CANDIDATES = ["controlled_terminology_demo.csv"]
STUDY_META_CANDIDATES = ["study_metadata_demo.json"]
PROGRAMMING_CONVENTIONS_CANDIDATES = ["programming_conventions_demo.json"]
SPONSOR_RULES_CANDIDATES = ["demo_sponsor_rules_dm.json"]

FINAL_COLUMNS = [
    "STUDYID", "DOMAIN", "USUBJID", "SUBJID", "SITEID",
    "RFSTDTC", "BRTHDTC", "AGE", "AGEU", "SEX",
    "RACE", "ETHNIC", "COUNTRY", "ARM", "ACTARM",
    "DTHFL", "DTHDTC",
]


def first_existing(base: Path, candidates: List[str], required: bool = True) -> Optional[Path]:
    for c in candidates:
        p = base / c
        if p.exists():
            return p
    if required:
        raise FileNotFoundError(f"None of these files were found: {candidates}")
    return None


def load_inputs():
    clean_source_path = first_existing(BASE, CLEAN_SOURCE_CANDIDATES)
    human_issues_path = first_existing(BASE, HUMAN_ISSUES_CANDIDATES, required=False)
    sdtm_fixable_path = first_existing(BASE, SDTM_FIXABLE_ISSUES_CANDIDATES, required=False)
    ct_path = first_existing(BASE, CONTROLLED_TERMS_CANDIDATES)
    meta_path = first_existing(BASE, STUDY_META_CANDIDATES)
    prog_path = first_existing(BASE, PROGRAMMING_CONVENTIONS_CANDIDATES)
    sponsor_rules_path = first_existing(BASE, SPONSOR_RULES_CANDIDATES)

    source_df = pd.read_csv(clean_source_path, dtype=str)
    human_issues_df = pd.read_csv(human_issues_path, dtype=str) if human_issues_path else pd.DataFrame(columns=["row_num", "field", "rule_id"])
    sdtm_fixable_df = pd.read_csv(sdtm_fixable_path, dtype=str) if sdtm_fixable_path else pd.DataFrame(columns=["row_num", "field", "rule_id"])
    ct_df = pd.read_csv(ct_path, dtype=str)
    study_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    prog_conv = json.loads(prog_path.read_text(encoding="utf-8"))
    sponsor_rules = json.loads(sponsor_rules_path.read_text(encoding="utf-8"))

    return source_df, human_issues_df, sdtm_fixable_df, ct_df, study_meta, prog_conv, sponsor_rules, {
        "clean_source": clean_source_path,
        "human_issues": human_issues_path,
        "sdtm_fixable": sdtm_fixable_path,
        "ct": ct_path,
        "meta": meta_path,
        "prog": prog_path,
        "sponsor_rules": sponsor_rules_path,
    }


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


def partial_sort_key(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if len(s) == 4:
        return s + "-01-01"
    if len(s) == 7:
        return s + "-01"
    if len(s) == 10:
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


def make_ct_lookup(ct_df):
    lut = {}
    cols = {str(c).strip().lower(): c for c in ct_df.columns}
    var_col = cols.get("variable") or cols.get("var") or cols.get("column") or cols.get("field") or cols.get("codelist_name")
    src_col = cols.get("source_value") or cols.get("source") or cols.get("raw_value") or cols.get("value") or cols.get("code") or cols.get("term")
    std_col = cols.get("standard_value") or cols.get("standard") or cols.get("mapped_value") or cols.get("target_value") or cols.get("normalized_value") or cols.get("value")
    if var_col is None or src_col is None or std_col is None:
        raise ValueError(f"CT file missing required columns. Found: {list(ct_df.columns)}")
    for _, row in ct_df.iterrows():
        var = str(row[var_col]).strip().upper()
        src = "" if pd.isna(row[src_col]) else str(row[src_col]).strip().upper()
        std = "" if pd.isna(row[std_col]) else str(row[std_col]).strip().upper()
        if var and src:
            lut[(var, src)] = std if std else src
    return lut


def apply_ct(variable: str, value: Optional[str], lut: Dict[tuple, str]) -> Optional[str]:
    if value is None:
        return None
    key = (variable.upper(), str(value).strip().upper())
    return lut.get(key, str(value).strip().upper())


def derive_ageu(value: Optional[str], age_present: bool, sponsor_rules: dict, lut: Dict[tuple, str]) -> Optional[str]:
    if value is not None:
        mapped = apply_ct("AGEU", value, lut)
        if mapped in {"YEAR", "YEARS", "YR", "YRS"}:
            return "YEARS"
        if mapped in {"MONTH", "MONTHS", "MO", "MOS"}:
            return "MONTHS"
        return mapped
    default = sponsor_rules.get("ageu_rule", {}).get("default_if_age_present")
    if age_present and default:
        return default
    return None


def derive_usubjid(row: dict, sponsor_rules: dict) -> Optional[str]:
    rule = sponsor_rules.get("usubjid_rule", "STUDYID-SITEID-SUBJID").upper()
    if row.get("USUBJID"):
        return row.get("USUBJID")
    if rule == "STUDYID-SITEID-SUBJID":
        if row.get("STUDYID") and row.get("SITEID") and row.get("SUBJID"):
            return f"{row['STUDYID']}-{row['SITEID']}-{row['SUBJID']}"
        return None
    if rule == "SUBJECT_KEY":
        return row.get("SUBJECT_KEY")
    return None


def get_row_value(row: pd.Series, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in row and row.get(c) not in [None, ""]:
            return row.get(c)
    return None


def row_to_issue_map(df: pd.DataFrame) -> Dict[int, Set[str]]:
    out: Dict[int, Set[str]] = {}
    if df is None or df.empty:
        return out
    temp = df.copy()
    temp["row_num"] = pd.to_numeric(temp["row_num"], errors="coerce")
    temp = temp.dropna(subset=["row_num"])
    for _, r in temp.iterrows():
        out.setdefault(int(r["row_num"]), set()).add(str(r.get("field", "")))
    return out


def row_has_human_issue(row_num: int, human_issue_map: Dict[int, Set[str]]) -> bool:
    return row_num in human_issue_map and len(human_issue_map[row_num]) > 0


def field_is_sdtm_fixable(row_num: int, field: str, sdtm_fixable_map: Dict[int, Set[str]]) -> bool:
    return row_num in sdtm_fixable_map and field in sdtm_fixable_map[row_num]

def normalized_sex_ok(val: Optional[str]) -> bool:
    if val is None:
        return False
    v = str(val).strip().upper()
    if v in {"M", "F", "U"}:
        return True
    return str(val).strip().lower() in {"male", "female", "unknown", "m", "f", "u"}


def normalized_dthfl_ok(val: Optional[str]) -> bool:
    if val is None:
        return False
    v = str(val).strip().upper()
    if v in {"Y", "N"}:
        return True
    return str(val).strip().lower() in {"yes", "no", "y", "n"}


def normalized_country_ok(val: Optional[str]) -> bool:
    if val is None:
        return False
    s = str(val).strip()
    if len(s) == 3 and s.isalpha():
        return True
    return s.upper() in {"INDIA", "UNITED STATES", "UK", "UNITED KINGDOM"}


def build_dm(source_df: pd.DataFrame, human_issues_df: pd.DataFrame, sdtm_fixable_df: pd.DataFrame,
             ct_lut: Dict[tuple, str], study_meta: dict, prog_conv: dict, sponsor_rules: dict):
    final_rows = []
    exception_rows = []
    qc_rows = []

    human_issue_map = row_to_issue_map(human_issues_df)
    sdtm_fixable_map = row_to_issue_map(sdtm_fixable_df)

    subjid_pattern = sponsor_rules.get("subjid_rule", {}).get("regex")
    subjid_re = __import__("re").compile(subjid_pattern) if subjid_pattern else None
    required_for_final = set(sponsor_rules.get("required_for_demo_final", ["STUDYID", "DOMAIN", "USUBJID", "SUBJID", "SITEID", "SEX"]))

    for i, src_row in source_df.iterrows():
        row_num = i + 1
        row_issues = []
        hard_fail = False
        out = {c: get_row_value(src_row, c) for c in FINAL_COLUMNS}
        original = out.copy()
        has_human = row_has_human_issue(row_num, human_issue_map)

        out["STUDYID"] = get_row_value(src_row, "STUDYID", "PROTOCOL_ID") or study_meta.get("study_id")
        out["DOMAIN"] = "DM"
        out["SUBJID"] = normalize_id(get_row_value(src_row, "SUBJID", "SUBJECT_NUMBER"))
        out["SITEID"] = normalize_id(get_row_value(src_row, "SITEID", "SITE_NUMBER"))
        out["RFSTDTC"] = to_iso_partial_date(get_row_value(src_row, "RFSTDTC", "REF_START_DT"))
        out["BRTHDTC"] = to_iso_partial_date(get_row_value(src_row, "BRTHDTC", "DATE_OF_BIRTH"))
        out["DTHDTC"] = to_iso_partial_date(get_row_value(src_row, "DTHDTC", "DEATH_DATE"))

        # Only apply SDTM-side fixes for fields explicitly classified as SDTM-fixable.
        collected_age = normalize_age_token(get_row_value(src_row, "AGE", "AGE_AT_REF"))
        if field_is_sdtm_fixable(row_num, "AGE", sdtm_fixable_map):
            if sponsor_rules.get("age_rule", {}).get("derive_if_full_dates_available", True) and is_full_date(out["BRTHDTC"]) and is_full_date(out["RFSTDTC"]):
                derived_age = calculate_age_years(out["BRTHDTC"], out["RFSTDTC"])
                out["AGE"] = derived_age if derived_age is not None else collected_age
            else:
                out["AGE"] = collected_age
        else:
            out["AGE"] = collected_age

        if field_is_sdtm_fixable(row_num, "AGEU", sdtm_fixable_map):
            out["AGEU"] = derive_ageu(get_row_value(src_row, "AGEU", "AGE_UNITS"), out["AGE"] is not None, sponsor_rules, ct_lut)
        else:
            out["AGEU"] = get_row_value(src_row, "AGEU", "AGE_UNITS")

        # Human-reviewed source values: preserve source-backed values; do not CT-standardize here.
        out["SEX"] = get_row_value(src_row, "SEX", "SEX_AT_BIRTH")
        out["RACE"] = apply_ct("RACE", get_row_value(src_row, "RACE", "RACE_CAT"), ct_lut) if get_row_value(src_row, "RACE", "RACE_CAT") is not None else None
        out["ETHNIC"] = apply_ct("ETHNIC", get_row_value(src_row, "ETHNIC", "ETHNIC_GRP"), ct_lut) if get_row_value(src_row, "ETHNIC", "ETHNIC_GRP") is not None else None
        out["COUNTRY"] = get_row_value(src_row, "COUNTRY", "COUNTRY_CODE")

        arm = get_row_value(src_row, "ARM", "PLANNED_TRT_ARM")
        actarm = get_row_value(src_row, "ACTARM", "ACTUAL_TRT_ARM")
        out["ARM"] = arm.upper() if isinstance(arm, str) and prog_conv.get("uppercase_char_fields", False) else arm
        out["ACTARM"] = actarm.upper() if isinstance(actarm, str) and prog_conv.get("uppercase_char_fields", False) else actarm

        out["DTHFL"] = get_row_value(src_row, "DTHFL", "DEATH_IND")

        helper_row = {**{k: src_row.get(k) for k in src_row.index}, **out}
        if field_is_sdtm_fixable(row_num, "USUBJID", sdtm_fixable_map):
            out["USUBJID"] = derive_usubjid(helper_row, sponsor_rules)
        else:
            out["USUBJID"] = get_row_value(src_row, "USUBJID") or derive_usubjid(helper_row, sponsor_rules)

        # Human issues block final inclusion, but SDTM-fixable fields can still be computed and recorded in exceptions/QC.
        if has_human:
            row_issues.append("Row has HUMAN_REVIEW issues from Layer 1; excluded from final SDTM until resolved")
            hard_fail = True

        for req in required_for_final:
            if not out.get(req):
                row_issues.append(f"{req} missing")
                hard_fail = True

        if out.get("SEX") and not normalized_sex_ok(out.get("SEX")):
            row_issues.append("SEX not in accepted source values")
            hard_fail = True

        if out.get("DTHFL") and not normalized_dthfl_ok(out.get("DTHFL")):
            row_issues.append("DTHFL not in accepted source values")
            hard_fail = True

        if out.get("COUNTRY") and not normalized_country_ok(out.get("COUNTRY")):
            row_issues.append("COUNTRY not in accepted source values")
            hard_fail = True

        if out.get("SUBJID") and subjid_re and not subjid_re.fullmatch(str(out["SUBJID"])):
            row_issues.append("SUBJID violates sponsor pattern")
            hard_fail = True

        if out.get("AGE") is not None:
            try:
                age_num = float(out["AGE"])
                if age_num < 0:
                    row_issues.append("AGE < 0")
                    hard_fail = True
                if age_num < 18:
                    row_issues.append("AGE below adult threshold")
                    hard_fail = True
                if age_num > 120:
                    row_issues.append("AGE implausibly high")
                    hard_fail = True
            except Exception:
                row_issues.append("AGE not numeric")
                hard_fail = True

        if out.get("AGE") is not None and sponsor_rules.get("ageu_rule", {}).get("default_if_age_present") == "YEARS":
            if out.get("AGEU") != "YEARS":
                row_issues.append("AGE present but AGEU not standardized to YEARS")
                # This is SDTM-fixable only if Layer 1 classified AGEU for SDTM.
                if not field_is_sdtm_fixable(row_num, "AGEU", sdtm_fixable_map):
                    hard_fail = True

        if out.get("DTHFL") == "Y" and not out.get("DTHDTC"):
            row_issues.append("DTHFL=Y but DTHDTC missing")
            hard_fail = True
        if out.get("DTHDTC") and out.get("DTHFL") not in {"Y", "y", "YES", "Yes", "yes"}:
            row_issues.append("DTHDTC populated but DTHFL not Y")
            hard_fail = True

        b = partial_sort_key(out.get("BRTHDTC"))
        r = partial_sort_key(out.get("RFSTDTC"))
        d = partial_sort_key(out.get("DTHDTC"))
        if sponsor_rules.get("date_rule", {}).get("preserve_partial_dates", True):
            if b and r and r < b:
                row_issues.append("RFSTDTC before BRTHDTC")
                hard_fail = True
            if r and d and d < r:
                row_issues.append("DTHDTC before RFSTDTC")
                hard_fail = True

        allowed_loss_fields = set(sponsor_rules.get("allowed_intentional_loss_fields", ["DTHFL"]))
        loss_fields = []
        for c in FINAL_COLUMNS:
            before = original.get(c)
            after = out.get(c)
            if before not in [None, ""] and after in [None, ""]:
                if c in allowed_loss_fields:
                    continue
                if c == "DTHDTC" and out.get("DTHFL") != "Y":
                    continue
                loss_fields.append(c)
        if loss_fields:
            row_issues.append("Unexpected data loss in fields: " + ", ".join(loss_fields))
            hard_fail = True

        # Anything that Layer 1 marked SDTM-fixable but still unresolved here becomes an SDTM exception.
        for fld in sorted(sdtm_fixable_map.get(row_num, set())):
            if fld == "AGE" and not out.get("AGE"):
                row_issues.append("Unresolved SDTM-fixable issue: AGE could not be derived/populated")
                hard_fail = True
            if fld == "AGEU" and out.get("AGE") and out.get("AGEU") != "YEARS":
                row_issues.append("Unresolved SDTM-fixable issue: AGEU not standardized to YEARS")
                hard_fail = True
            if fld == "USUBJID" and not out.get("USUBJID"):
                row_issues.append("Unresolved SDTM-fixable issue: USUBJID could not be derived")
                hard_fail = True
            if fld in {"SITEID", "SUBJID"} and not out.get(fld):
                row_issues.append(f"Unresolved SDTM-fixable issue: {fld} could not be populated")
                hard_fail = True

        exception_record = {c: out.get(c) for c in FINAL_COLUMNS}
        exception_record["SOURCE_ROW_NUMBER"] = row_num
        exception_record["ROW_ISSUES"] = " | ".join(dict.fromkeys(row_issues))
        exception_record["HAS_HUMAN_REVIEW_ISSUE"] = "Y" if has_human else "N"
        exception_record["HAS_SDTM_FIXABLE_ISSUE"] = "Y" if row_num in sdtm_fixable_map else "N"

        qc_rows.append({
            "SOURCE_ROW_NUMBER": row_num,
            "USUBJID": out.get("USUBJID"),
            "ISSUE_COUNT": len(dict.fromkeys(row_issues)),
            "ISSUES": " | ".join(dict.fromkeys(row_issues)),
            "HAS_HUMAN_REVIEW_ISSUE": "Y" if has_human else "N",
            "HAS_SDTM_FIXABLE_ISSUE": "Y" if row_num in sdtm_fixable_map else "N",
            "DISPOSITION": "EXCEPTION" if hard_fail else ("PASS_WITH_WARNINGS" if row_issues else "PASS"),
        })

        if hard_fail:
            exception_rows.append(exception_record)
        else:
            final_rows.append({c: out.get(c) for c in FINAL_COLUMNS})

    final_df = pd.DataFrame(final_rows)
    exceptions_df = pd.DataFrame(exception_rows)
    qc_df = pd.DataFrame(qc_rows)

    if not final_df.empty and "USUBJID" in final_df.columns:
        dup_mask = final_df["USUBJID"].duplicated(keep=False)
        if dup_mask.any():
            dup_rows = final_df.loc[dup_mask].copy()
            dup_rows["SOURCE_ROW_NUMBER"] = ""
            dup_rows["ROW_ISSUES"] = "Duplicate USUBJID in final DM"
            dup_rows["HAS_HUMAN_REVIEW_ISSUE"] = "N"
            dup_rows["HAS_SDTM_FIXABLE_ISSUE"] = "N"
            exceptions_df = pd.concat([exceptions_df, dup_rows], ignore_index=True)
            final_df = final_df.loc[~dup_mask].copy()

    return final_df, exceptions_df, qc_df


def write_outputs(final_df, exceptions_df, qc_df, used):
    OUTPUT_DIR.mkdir(exist_ok=True)
    final_df = final_df.reindex(columns=FINAL_COLUMNS)
    final_df.to_csv(OUTPUT_DIR / "dm_sdtm_final_v5.csv", index=False)
    exceptions_df.to_csv(OUTPUT_DIR / "dm_sdtm_exceptions_v5.csv", index=False)
    qc_df.to_csv(OUTPUT_DIR / "dm_sdtm_qc_report_v5.csv", index=False)

    readme = (
        "SDTM DM V5 build notes:\n"
        "- Reads Layer 1 cleaned DM plus issue-routing outputs.\n"
        "- Excludes any row with HUMAN_REVIEW issues from final SDTM.\n"
        "- Applies only explicitly SDTM-standardizable fixes to the relevant fields.\n"
        "- If a row has both HUMAN and SDTM-fixable issues, SDTM fixes are attempted but the row remains excluded until human items are resolved.\n"
        "- Any unresolved SDTM-fixable items are pushed to dm_sdtm_exceptions_v5.csv.\n\n"
        f"Layer 1 source used: {used['clean_source'].name}\n"
        f"Human issues used: {used['human_issues'].name if used['human_issues'] else 'None'}\n"
        f"SDTM-fixable issues used: {used['sdtm_fixable'].name if used['sdtm_fixable'] else 'None'}\n"
        f"Controlled terminology used: {used['ct'].name}\n"
        f"Study metadata used: {used['meta'].name}\n"
        f"Programming conventions used: {used['prog'].name}\n"
        f"Sponsor rules used: {used['sponsor_rules'].name}\n"
    )
    (OUTPUT_DIR / "README.txt").write_text(readme, encoding="utf-8")


def main():
    source_df, human_issues_df, sdtm_fixable_df, ct_df, study_meta, prog_conv, sponsor_rules, used = load_inputs()
    source_df = normalize_df(source_df, prog_conv)
    ct_lut = make_ct_lookup(ct_df)
    final_df, exceptions_df, qc_df = build_dm(source_df, human_issues_df, sdtm_fixable_df, ct_lut, study_meta, prog_conv, sponsor_rules)
    write_outputs(final_df, exceptions_df, qc_df, used)
    print(f"Created outputs in: {OUTPUT_DIR}")
    print("- dm_sdtm_final_v5.csv")
    print("- dm_sdtm_exceptions_v5.csv")
    print("- dm_sdtm_qc_report_v5.csv")


if __name__ == "__main__":
    main()
