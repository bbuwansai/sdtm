
import json
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import pandas as pd

BASE = Path(__file__).resolve().parent
OUTPUT_DIR = BASE / "sdtm_outputs_v4"

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

FINAL_COLUMNS = [
    "STUDYID", "DOMAIN", "USUBJID", "SUBJID", "SITEID",
    "RFSTDTC", "BRTHDTC", "AGE", "AGEU", "SEX",
    "RACE", "ETHNIC", "COUNTRY", "ARM", "ACTARM",
    "DTHFL", "DTHDTC",
]

def first_existing(base: Path, candidates: List[str]) -> Path:
    for c in candidates:
        p = base / c
        if p.exists():
            return p
    raise FileNotFoundError(f"None of these files were found: {candidates}")

def load_inputs():
    clean_source_path = first_existing(BASE, CLEAN_SOURCE_CANDIDATES)
    ct_path = first_existing(BASE, CONTROLLED_TERMS_CANDIDATES)
    meta_path = first_existing(BASE, STUDY_META_CANDIDATES)
    prog_path = first_existing(BASE, PROGRAMMING_CONVENTIONS_CANDIDATES)
    sponsor_rules_path = first_existing(BASE, SPONSOR_RULES_CANDIDATES)

    source_df = pd.read_csv(clean_source_path, dtype=str)
    ct_df = pd.read_csv(ct_path, dtype=str)
    study_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    prog_conv = json.loads(prog_path.read_text(encoding="utf-8"))
    sponsor_rules = json.loads(sponsor_rules_path.read_text(encoding="utf-8"))

    return source_df, ct_df, study_meta, prog_conv, sponsor_rules, {
        "clean_source": clean_source_path,
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

    # Normalize column names
    cols = {c.lower(): c for c in ct_df.columns}

    var_col = cols.get("variable") or cols.get("var") or cols.get("column") or cols.get("field")
    val_col = cols.get("value") or cols.get("code") or cols.get("term")

    if var_col is None or val_col is None:
        raise ValueError(f"CT file missing required columns. Found: {list(ct_df.columns)}")

    for _, row in ct_df.iterrows():
        var = str(row[var_col]).strip().upper()
        val = str(row[val_col]).strip().upper()

        if var not in lut:
            lut[var] = set()

        lut[var].add(val)

    return lut

def apply_ct(variable: str, value: Optional[str], lut: Dict[tuple, str]) -> Optional[str]:
    if value is None:
        return None
    key = (variable.upper(), str(value).strip().upper())
    return lut.get(key, str(value).strip().upper())

def standardize_ageu(value: Optional[str], age_present: bool, sponsor_rules: dict, lut: Dict[tuple, str]) -> Optional[str]:
    if value is not None:
        mapped = apply_ct("AGEU", value, lut)
        if mapped in {"YEAR", "YEARS", "YR", "YRS"}:
            return "YEARS"
        return mapped
    default = sponsor_rules.get("ageu_rule", {}).get("default_if_age_present")
    if age_present and default:
        return default
    return None

def standardize_country(value: Optional[str], sponsor_rules: dict, lut: Dict[tuple, str]) -> Optional[str]:
    if value is None:
        return None
    v = apply_ct("COUNTRY", value, lut)
    if len(v) == 3:
        return v
    if sponsor_rules.get("country_rule", {}).get("pass_through_if_already_iso3") and len(str(value).strip()) == 3:
        return str(value).strip().upper()
    return v

def standardize_dthfl(dthfl_val: Optional[str], dthdtc_val: Optional[str], sponsor_rules: dict, lut: Dict[tuple, str]) -> Optional[str]:
    if dthdtc_val and sponsor_rules.get("dthfl_rule", {}).get("set_Y_if_dthdtc_present", True):
        return "Y"
    if dthfl_val is None:
        return None
    mapped = apply_ct("DTHFL", dthfl_val, lut)
    if mapped in {"Y", "YES"}:
        return "Y"
    # sponsor rule: Y or blank only
    if sponsor_rules.get("dthfl_rule", {}).get("allow_only_Y_or_blank", True):
        return None
    return mapped

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

def build_dm(source_df: pd.DataFrame, ct_lut: Dict[tuple, str], study_meta: dict, prog_conv: dict, sponsor_rules: dict):
    final_rows = []
    exception_rows = []
    qc_rows = []

    subjid_pattern = sponsor_rules.get("subjid_rule", {}).get("regex")
    if subjid_pattern:
        import re
        subjid_re = re.compile(subjid_pattern)
    else:
        subjid_re = None

    required_for_final = set(sponsor_rules.get("required_for_demo_final", ["STUDYID","DOMAIN","USUBJID","SUBJID","SITEID","SEX"]))

    for i, src_row in source_df.iterrows():
        row_issues = []
        hard_fail = False

        # Start from Layer 1 as source of truth. Preserve values unless standardization/derivation explicitly changes them.
        out = {c: get_row_value(src_row, c) for c in FINAL_COLUMNS}
        original = out.copy()

        out["STUDYID"] = get_row_value(src_row, "STUDYID", "PROTOCOL_ID") or study_meta.get("study_id")
        out["DOMAIN"] = "DM"
        out["SUBJID"] = normalize_id(get_row_value(src_row, "SUBJID", "SUBJECT_NUMBER"))
        out["SITEID"] = normalize_id(get_row_value(src_row, "SITEID", "SITE_NUMBER"))

        # Dates: preserve valid partial dates; do not impute.
        out["RFSTDTC"] = to_iso_partial_date(get_row_value(src_row, "RFSTDTC", "REF_START_DT"))
        out["BRTHDTC"] = to_iso_partial_date(get_row_value(src_row, "BRTHDTC", "DATE_OF_BIRTH"))
        out["DTHDTC"] = to_iso_partial_date(get_row_value(src_row, "DTHDTC", "DEATH_DATE"))

        # AGE: sponsor rule says derive if full DOB and full RFSTDTC available, else keep collected.
        collected_age = normalize_age_token(get_row_value(src_row, "AGE", "AGE_AT_REF"))
        if sponsor_rules.get("age_rule", {}).get("derive_if_full_dates_available", True) and is_full_date(out["BRTHDTC"]) and is_full_date(out["RFSTDTC"]):
            derived_age = calculate_age_years(out["BRTHDTC"], out["RFSTDTC"])
            if derived_age is not None:
                out["AGE"] = derived_age
            else:
                out["AGE"] = collected_age
                if collected_age is None:
                    row_issues.append("AGE unavailable: could not derive and no collected AGE")
        else:
            out["AGE"] = collected_age

        # AGEU: use sponsor default when AGE exists
        out["AGEU"] = standardize_ageu(get_row_value(src_row, "AGEU", "AGE_UNITS"), out["AGE"] is not None, sponsor_rules, ct_lut)

        # CT standardization
        out["SEX"] = apply_ct("SEX", get_row_value(src_row, "SEX", "SEX_AT_BIRTH"), ct_lut) if get_row_value(src_row, "SEX", "SEX_AT_BIRTH") is not None else None
        out["RACE"] = apply_ct("RACE", get_row_value(src_row, "RACE", "RACE_CAT"), ct_lut) if get_row_value(src_row, "RACE", "RACE_CAT") is not None else None
        out["ETHNIC"] = apply_ct("ETHNIC", get_row_value(src_row, "ETHNIC", "ETHNIC_GRP"), ct_lut) if get_row_value(src_row, "ETHNIC", "ETHNIC_GRP") is not None else None
        out["COUNTRY"] = standardize_country(get_row_value(src_row, "COUNTRY", "COUNTRY_CODE"), sponsor_rules, ct_lut)

        arm = get_row_value(src_row, "ARM", "PLANNED_TRT_ARM")
        actarm = get_row_value(src_row, "ACTARM", "ACTUAL_TRT_ARM")
        out["ARM"] = arm.upper() if isinstance(arm, str) and prog_conv.get("uppercase_char_fields", False) else arm
        out["ACTARM"] = actarm.upper() if isinstance(actarm, str) and prog_conv.get("uppercase_char_fields", False) else actarm

        out["DTHFL"] = standardize_dthfl(get_row_value(src_row, "DTHFL", "DEATH_IND"), out["DTHDTC"], sponsor_rules, ct_lut)

        # USUBJID last, after STUDYID/SITEID/SUBJID are standardized
        helper_row = {
            **{k: src_row.get(k) for k in src_row.index},
            **out
        }
        out["USUBJID"] = derive_usubjid(helper_row, sponsor_rules)

        # -------- Validation / gating --------
        for req in required_for_final:
            if not out.get(req):
                row_issues.append(f"{req} missing")
                hard_fail = True

        # Study-specific SUBJID pattern only if sponsor says so
        if out.get("SUBJID") and subjid_re and not subjid_re.fullmatch(str(out["SUBJID"])):
            row_issues.append("SUBJID violates sponsor pattern")
            hard_fail = True

        # AGE numeric if present
        if out.get("AGE") is not None:
            try:
                age_num = float(out["AGE"])
                if age_num < 0:
                    row_issues.append("AGE < 0")
                    hard_fail = True
            except Exception:
                row_issues.append("AGE not numeric")
                hard_fail = True

        # AGEU rule
        if out.get("AGE") is not None and sponsor_rules.get("ageu_rule", {}).get("default_if_age_present") == "YEARS":
            if out.get("AGEU") != "YEARS":
                row_issues.append("AGE present but AGEU not standardized to YEARS")
                hard_fail = True

        # COUNTRY rule
        if out.get("COUNTRY") is not None:
            if sponsor_rules.get("country_rule", {}).get("target_standard") == "ISO 3166-1 alpha-3" and len(str(out["COUNTRY"])) != 3:
                row_issues.append("COUNTRY not ISO alpha-3")
                hard_fail = True

        # DTHFL/DTHDTC consistency
        if out.get("DTHFL") == "Y" and not out.get("DTHDTC") and sponsor_rules.get("dthfl_rule", {}).get("set_Y_if_dthdtc_present", True):
            row_issues.append("DTHFL=Y but DTHDTC missing")
            hard_fail = True
        if out.get("DTHDTC") and out.get("DTHFL") != "Y":
            row_issues.append("DTHDTC populated but DTHFL not Y")
            hard_fail = True

        # Chronology checks
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

        # Loss check: no unexpected loss versus Layer 1
        allowed_loss_fields = set(sponsor_rules.get("allowed_intentional_loss_fields", ["DTHFL"]))
        loss_fields = []
        for c in FINAL_COLUMNS:
            before = original.get(c)
            after = out.get(c)
            if before not in [None, ""] and after in [None, ""]:
                if c in allowed_loss_fields:
                    continue
                loss_fields.append(c)
        if loss_fields:
            row_issues.append("Unexpected data loss in fields: " + ", ".join(loss_fields))
            hard_fail = True

        record = {c: out.get(c) for c in FINAL_COLUMNS}
        record["SOURCE_ROW_NUMBER"] = i + 1
        record["ROW_ISSUES"] = " | ".join(row_issues)

        qc_rows.append({
            "SOURCE_ROW_NUMBER": i + 1,
            "USUBJID": out.get("USUBJID"),
            "ISSUE_COUNT": len(row_issues),
            "ISSUES": " | ".join(row_issues),
            "DISPOSITION": "EXCEPTION" if hard_fail else ("PASS_WITH_WARNINGS" if row_issues else "PASS"),
        })

        if hard_fail:
            exception_rows.append(record)
        else:
            final_rows.append(record)

    final_df = pd.DataFrame(final_rows)
    exceptions_df = pd.DataFrame(exception_rows)
    qc_df = pd.DataFrame(qc_rows)

    # Dataset-level uniqueness check in final
    if not final_df.empty and "USUBJID" in final_df.columns:
        dup_mask = final_df["USUBJID"].duplicated(keep=False)
        if dup_mask.any():
            dup_rows = final_df.loc[dup_mask].copy()
            dup_rows["ROW_ISSUES"] = dup_rows["ROW_ISSUES"].fillna("") + " | Duplicate USUBJID in final DM"
            exceptions_df = pd.concat([exceptions_df, dup_rows], ignore_index=True)
            final_df = final_df.loc[~dup_mask].copy()

    return final_df, exceptions_df, qc_df

def write_outputs(final_df, exceptions_df, qc_df, used):
    OUTPUT_DIR.mkdir(exist_ok=True)
    final_df.to_csv(OUTPUT_DIR / "dm_sdtm_final_v4.csv", index=False)
    exceptions_df.to_csv(OUTPUT_DIR / "dm_sdtm_exceptions_v4.csv", index=False)
    qc_df.to_csv(OUTPUT_DIR / "dm_sdtm_qc_report_v4.csv", index=False)

    readme = (
        "SDTM DM V4 build notes:\n"
        "- Starts from Layer 1 cleaned DM source.\n"
        "- Applies sponsor-approved SDTM standardization and derivation rules.\n"
        "- Preserves valid Layer 1 values and prevents unexpected data loss.\n"
        "- Uses sponsor rule file as strict transformation policy.\n\n"
        f"Layer 1 source used: {used['clean_source'].name}\n"
        f"Controlled terminology used: {used['ct'].name}\n"
        f"Study metadata used: {used['meta'].name}\n"
        f"Programming conventions used: {used['prog'].name}\n"
        f"Sponsor rules used: {used['sponsor_rules'].name}\n"
    )
    (OUTPUT_DIR / "README.txt").write_text(readme, encoding="utf-8")

def main():
    source_df, ct_df, study_meta, prog_conv, sponsor_rules, used = load_inputs()
    source_df = normalize_df(source_df, prog_conv)
    ct_lut = make_ct_lookup(ct_df)

    final_df, exceptions_df, qc_df = build_dm(source_df, ct_lut, study_meta, prog_conv, sponsor_rules)
    write_outputs(final_df, exceptions_df, qc_df, used)

    print(f"Created outputs in: {OUTPUT_DIR}")
    print("- dm_sdtm_final_v4.csv")
    print("- dm_sdtm_exceptions_v4.csv")
    print("- dm_sdtm_qc_report_v4.csv")

if __name__ == "__main__":
    main()
