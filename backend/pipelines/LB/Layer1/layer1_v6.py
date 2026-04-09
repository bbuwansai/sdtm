
import argparse
import json
import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

NULLS = {"", "NA", "NAN", "NONE", "NULL"}
EXPECTED_COLUMNS = [
    "PROTOCOL_NO","SITE_NO","SUBJECT_NO","SEX_RAW","AGE_YRS","LB_PAGE_ID","LB_LINE_NO",
    "LAB_SOURCE_RAW","LAB_VENDOR_RAW","FORM_NAME","VISIT_RAW","VISITNUM_RAW","UNSCHED_RAW",
    "VISITDT_RAW","COLL_DATE_RAW","COLL_TIME_RAW","COLL_DTM_RAW","FASTING_RAW","POSTDOSE_RAW",
    "SPECIMEN_RAW","TEST_PANEL_RAW","TEST_NAME_RAW","TEST_CODE_RAW","RESULT_RAW","RESULT_NUM_RAW",
    "RESULT_CHAR_RAW","RESULT_QUAL_RAW","ORIG_UNIT_RAW","REF_LOW_RAW","REF_HIGH_RAW",
    "REF_RANGE_TEXT_RAW","REF_UNIT_RAW","ABN_FLAG_RAW","CLIN_SIG_RAW","NOT_DONE_RAW","ND_REASON_RAW",
    "HEMOLYZED_RAW","REPEAT_RAW","SAMPLE_ID_RAW","COMMENT_RAW"
]

def clean(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    return None if s.upper() in NULLS else s

def upper_clean(v):
    s = clean(v)
    return s.upper() if isinstance(s, str) else s

def present(v):
    return v is not None and not pd.isna(v)

def as_text(v):
    if pd.isna(v):
        return ""
    return str(v).strip()

def parse_num(v: Optional[str]):
    if v is None:
        return None
    s = str(v).strip()
    if re.fullmatch(r"-?\d+(?:\.\d+)?", s):
        try:
            return float(s)
        except Exception:
            return None
    return None

def canonical_num_string(v: Optional[str]) -> Optional[str]:
    n = parse_num(v)
    if n is None:
        return None
    return str(int(n)) if float(n).is_integer() else str(n)

def normalize_partial_iso(s: str) -> Optional[str]:
    s = s.strip()
    if re.fullmatch(r"\d{4}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            pd.to_datetime(s, format="%Y-%m-%d", errors="raise")
            return s
        except Exception:
            return None
    return None

def parse_date_value(v: Optional[str]) -> Tuple[Optional[str], str]:
    if v is None:
        return None, "missing"
    s = str(v).strip()
    iso = normalize_partial_iso(s)
    if iso is not None:
        return iso, "iso_valid"
    for fmt in ("%Y/%m/%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            return ts.strftime("%Y-%m-%d"), "alt_valid"
        except Exception:
            pass
    return None, "invalid"

def parse_time_value(v: Optional[str]) -> Tuple[Optional[str], str]:
    if v is None:
        return None, "missing"
    s = str(v).strip()
    if re.fullmatch(r"\d{2}:\d{2}", s):
        hh, mm = int(s[:2]), int(s[3:])
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return s, "std_valid"
        return None, "invalid"
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}", s):
        hh, mm, ss = map(int, s.split(":"))
        if 0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59:
            return s[:5], "alt_valid"
        return None, "invalid"
    for fmt in ("%I:%M %p", "%H%M"):
        try:
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            return ts.strftime("%H:%M"), "alt_valid"
        except Exception:
            pass
    return None, "invalid"

def parse_dtm_value(v: Optional[str]) -> Tuple[Optional[str], str]:
    if v is None:
        return None, "missing"
    s = str(v).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?", s):
        try:
            fmt = "%Y-%m-%dT%H:%M:%S" if len(s) == 19 else "%Y-%m-%dT%H:%M"
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            return ts.strftime("%Y-%m-%dT%H:%M:%S") if len(s) == 19 else ts.strftime("%Y-%m-%dT%H:%M"), "std_valid"
        except Exception:
            return None, "invalid"
    for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M", "%d-%b-%Y %H:%M"):
        try:
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            return ts.strftime("%Y-%m-%dT%H:%M"), "alt_valid"
        except Exception:
            pass
    return None, "invalid"

def infer_unsched(visit_raw, patterns):
    if visit_raw is None:
        return None
    v = str(visit_raw).upper()
    return "Y" if any(p in v for p in patterns) else "N"

def add_issue(issues, summary, rule_meta, rule_id, row_idx=None, row=None,
              variable_name=None, variable_value=None, message_override=None):
    meta = rule_meta[rule_id]
    bucket = meta["bucket"]
    issues.append({
        "source_row_number": None if row_idx is None else int(row.get("L1_SOURCE_ROW_NUMBER", row_idx + 1)),
        "rule_id": rule_id,
        "severity": meta["severity"],
        "final_bucket": bucket,
        "rule_description": meta["description"],
        "classification_basis": meta["basis"],
        "subject_key": None if row is None else row.get("SUBJECT_KEY"),
        "visit_raw": None if row is None else row.get("VISIT_RAW"),
        "visitnum_raw": None if row is None else row.get("VISITNUM_RAW"),
        "test_code": None if row is None else row.get("TEST_CODE_RAW"),
        "test_name": None if row is None else row.get("TEST_NAME_RAW"),
        "variable_name": variable_name,
        "variable_value": variable_value,
        "message": message_override or meta["description"],
    })
    summary[(rule_id, meta["severity"], bucket, meta["description"])] = summary.get(
        (rule_id, meta["severity"], bucket, meta["description"]), 0
    ) + 1

def main():
    parser = argparse.ArgumentParser(description="LB Layer 1 QC v5")
    parser.add_argument("--source", help="Path to LB raw CSV input")
    parser.add_argument("--rules", help="Path to rules JSON")
    parser.add_argument("--outdir", help="Optional output directory override")
    args = parser.parse_args()

    base = Path(__file__).resolve().parent
        if args.rules:
        rules_path = Path(args.rules)
    else:
        candidates = [
            base / "lb_layer1_rules_v5.json",
            base / "lb_layer1_rules_v5_1.json",
            base / "lb_layer1_rules.json",
        ]
        rules_path = next((p for p in candidates if p.exists()), candidates[0])

    if not rules_path.exists():
        raise FileNotFoundError(f"Rules JSON not found. Tried: {[str(p) for p in candidates] if not args.rules else [str(rules_path)]}"))

    cfg = json.loads(rules_path.read_text(encoding="utf-8"))
    source = Path(args.source) if args.source else (base / cfg["input"]["source_csv"])
    if not source.exists():
        raise FileNotFoundError(
            f"Input CSV not found: {source}\n"
            f"Pass --source or update source_csv in {rules_path.name}."
        )

    outdir = Path(args.outdir) if args.outdir else (base / cfg["input"]["output_dir"])
    outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(source, dtype=str)
    missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError("Input file is missing required columns: " + ", ".join(missing_cols))

    if "L1_SOURCE_ROW_NUMBER" not in df.columns:
        df.insert(0, "L1_SOURCE_ROW_NUMBER", range(1, len(df) + 1))

    for c in df.columns:
        df[c] = df[c].apply(clean)

    upper_cols = [
        "PROTOCOL_NO","SITE_NO","SUBJECT_NO","SEX_RAW","LAB_SOURCE_RAW","LAB_VENDOR_RAW","FORM_NAME",
        "VISIT_RAW","VISITNUM_RAW","UNSCHED_RAW","FASTING_RAW","POSTDOSE_RAW","SPECIMEN_RAW","TEST_PANEL_RAW",
        "TEST_NAME_RAW","TEST_CODE_RAW","RESULT_RAW","RESULT_NUM_RAW","RESULT_CHAR_RAW","RESULT_QUAL_RAW",
        "ORIG_UNIT_RAW","REF_RANGE_TEXT_RAW","REF_UNIT_RAW","ABN_FLAG_RAW","CLIN_SIG_RAW","NOT_DONE_RAW",
        "ND_REASON_RAW","HEMOLYZED_RAW","REPEAT_RAW","SAMPLE_ID_RAW","COMMENT_RAW"
    ]
    for col in upper_cols:
        df[col] = df[col].apply(upper_clean)

    df["SUBJECT_KEY"] = df[["PROTOCOL_NO","SITE_NO","SUBJECT_NO"]].fillna("").agg("|".join, axis=1)
    df["AGE_NUM"] = df["AGE_YRS"].apply(parse_num)
    df["VISITNUM_CANON"] = df["VISITNUM_RAW"].apply(canonical_num_string)
    df["RESULT_NUM_PARSED"] = df["RESULT_NUM_RAW"].apply(parse_num)
    df["RESULT_RAW_PARSED"] = df["RESULT_RAW"].apply(parse_num)
    df["REF_LOW_NUM"] = df["REF_LOW_RAW"].apply(parse_num)
    df["REF_HIGH_NUM"] = df["REF_HIGH_RAW"].apply(parse_num)

    df[["VISITDT_NORM","VISITDT_STATUS"]] = df["VISITDT_RAW"].apply(lambda x: pd.Series(parse_date_value(x)))
    df[["COLL_DATE_NORM","COLL_DATE_STATUS"]] = df["COLL_DATE_RAW"].apply(lambda x: pd.Series(parse_date_value(x)))
    df[["COLL_TIME_NORM","COLL_TIME_STATUS"]] = df["COLL_TIME_RAW"].apply(lambda x: pd.Series(parse_time_value(x)))
    df[["COLL_DTM_NORM","COLL_DTM_STATUS"]] = df["COLL_DTM_RAW"].apply(lambda x: pd.Series(parse_dtm_value(x)))

    issues = []
    summary = {}
    rule_meta = cfg["rules"]

    for idx, row in df.iterrows():
        for col in cfg["required_fields_always"]:
            if row.get(col) is None:
                add_issue(issues, summary, rule_meta, "LB001", idx, row, col, None, f"{col} is required at Layer 1.")

    if df["PROTOCOL_NO"].dropna().nunique() > 1:
        for idx, row in df.iterrows():
            add_issue(issues, summary, rule_meta, "LB002", idx, row, "PROTOCOL_NO", row.get("PROTOCOL_NO"))

    allowed_sex = set(cfg["allowed"]["sex"])
    for idx, row in df.iterrows():
        if row.get("SEX_RAW") is not None and row["SEX_RAW"] not in allowed_sex:
            add_issue(issues, summary, rule_meta, "LB003", idx, row, "SEX_RAW", row.get("SEX_RAW"))

    sex_counts = df[["SUBJECT_KEY","SEX_RAW"]].dropna().drop_duplicates().groupby("SUBJECT_KEY")["SEX_RAW"].nunique()
    for sk, n in sex_counts.items():
        if n > 1:
            for idx, row in df[df["SUBJECT_KEY"] == sk].iterrows():
                add_issue(issues, summary, rule_meta, "LB004", idx, row, "SEX_RAW", row.get("SEX_RAW"))

    amin, amax = cfg["subject_rules"]["age_min"], cfg["subject_rules"]["age_max"]
    for idx, row in df.iterrows():
        age = row.get("AGE_NUM")
        if age is None or not (amin <= age <= amax):
            add_issue(issues, summary, rule_meta, "LB005", idx, row, "AGE_YRS", row.get("AGE_YRS"))

    age_counts = df[["SUBJECT_KEY","AGE_NUM"]].dropna().drop_duplicates().groupby("SUBJECT_KEY")["AGE_NUM"].nunique()
    for sk, n in age_counts.items():
        if n > 1:
            for idx, row in df[df["SUBJECT_KEY"] == sk].iterrows():
                add_issue(issues, summary, rule_meta, "LB006", idx, row, "AGE_YRS", row.get("AGE_YRS"))

    dup_page_line = df.duplicated(subset=["SUBJECT_KEY","LB_PAGE_ID","LB_LINE_NO"], keep=False)
    for idx, row in df[dup_page_line & df["LB_PAGE_ID"].notna() & df["LB_LINE_NO"].notna()].iterrows():
        add_issue(issues, summary, rule_meta, "LB007", idx, row, "LB_PAGE_ID/LB_LINE_NO", f"{row.get('LB_PAGE_ID')}/{row.get('LB_LINE_NO')}")

    non_missing_forms = df["FORM_NAME"].dropna()
    if len(non_missing_forms) == 0:
        for idx, row in df.iterrows():
            add_issue(issues, summary, rule_meta, "LB008", idx, row, "FORM_NAME", row.get("FORM_NAME"), "FORM_NAME is missing.")
    elif non_missing_forms.nunique() > 1:
        for idx, row in df.iterrows():
            add_issue(issues, summary, rule_meta, "LB009", idx, row, "FORM_NAME", row.get("FORM_NAME"), "FORM_NAME varies within dataset.")

    visit_map = {k.upper(): str(v) for k, v in cfg["visit_rules"]["expected_visitnum_by_visitname"].items()}
    unsched_patterns = [x.upper() for x in cfg["unsched_patterns"]]
    for idx, row in df.iterrows():
        vr = row.get("VISIT_RAW")
        vn = row.get("VISITNUM_CANON")
        uns = row.get("UNSCHED_RAW")
        if vr in visit_map and vn is not None and vn != visit_map[vr]:
            add_issue(issues, summary, rule_meta, "LB010", idx, row, "VISITNUM_RAW", row.get("VISITNUM_RAW"))
        if uns is not None and uns not in set(cfg["allowed"]["yn"]):
            add_issue(issues, summary, rule_meta, "LB011", idx, row, "UNSCHED_RAW", uns)
        inferred = infer_unsched(vr, unsched_patterns)
        if uns is None:
            add_issue(issues, summary, rule_meta, "LB012", idx, row, "UNSCHED_RAW", uns, f"VISIT_RAW suggests UNSCHED_RAW={inferred}.")
        elif inferred is not None and "UNSCHED" in (vr or "") and inferred != uns:
            add_issue(issues, summary, rule_meta, "LB013", idx, row, "UNSCHED_RAW", uns, f"VISIT_RAW suggests UNSCHED_RAW={inferred}.")

    for idx, row in df.iterrows():
        if row["VISITDT_STATUS"] == "alt_valid":
            add_issue(issues, summary, rule_meta, "LB014", idx, row, "VISITDT_RAW", row.get("VISITDT_RAW"), f"Could standardize to {row['VISITDT_NORM']}.")
        elif row["VISITDT_STATUS"] == "invalid":
            add_issue(issues, summary, rule_meta, "LB015", idx, row, "VISITDT_RAW", row.get("VISITDT_RAW"))

        if row["COLL_DATE_STATUS"] == "alt_valid":
            add_issue(issues, summary, rule_meta, "LB016", idx, row, "COLL_DATE_RAW", row.get("COLL_DATE_RAW"), f"Could standardize to {row['COLL_DATE_NORM']}.")
        elif row["COLL_DATE_STATUS"] == "invalid":
            add_issue(issues, summary, rule_meta, "LB017", idx, row, "COLL_DATE_RAW", row.get("COLL_DATE_RAW"))

        if row["COLL_TIME_STATUS"] == "alt_valid":
            add_issue(issues, summary, rule_meta, "LB018", idx, row, "COLL_TIME_RAW", row.get("COLL_TIME_RAW"), f"Could standardize to {row['COLL_TIME_NORM']}.")
        elif row["COLL_TIME_STATUS"] == "invalid":
            add_issue(issues, summary, rule_meta, "LB019", idx, row, "COLL_TIME_RAW", row.get("COLL_TIME_RAW"))

        if row["COLL_DTM_STATUS"] == "alt_valid":
            add_issue(issues, summary, rule_meta, "LB020", idx, row, "COLL_DTM_RAW", row.get("COLL_DTM_RAW"), f"Could standardize to {row['COLL_DTM_NORM']}.")
        elif row["COLL_DTM_STATUS"] == "invalid":
            add_issue(issues, summary, rule_meta, "LB021", idx, row, "COLL_DTM_RAW", row.get("COLL_DTM_RAW"))

        cdate, ctime, cdtm = row.get("COLL_DATE_NORM"), row.get("COLL_TIME_NORM"), row.get("COLL_DTM_NORM")
        if cdate and ctime and cdtm:
            expected = f"{cdate}T{ctime}"
            if not str(cdtm).startswith(expected):
                add_issue(issues, summary, rule_meta, "LB022", idx, row, "COLL_DTM_RAW", row.get("COLL_DTM_RAW"), f"Expected prefix {expected}.")

    for idx, row in df.iterrows():
        fasting = row.get("FASTING_RAW")
        if fasting is None:
            add_issue(issues, summary, rule_meta, "LB023", idx, row, "FASTING_RAW", fasting)
        elif fasting not in set(cfg["allowed"]["fasting_values"]):
            add_issue(issues, summary, rule_meta, "LB024", idx, row, "FASTING_RAW", fasting)

        postdose = row.get("POSTDOSE_RAW")
        if postdose is None:
            add_issue(issues, summary, rule_meta, "LB025", idx, row, "POSTDOSE_RAW", postdose)
        elif postdose not in set(cfg["allowed"]["postdose_values"]):
            add_issue(issues, summary, rule_meta, "LB026", idx, row, "POSTDOSE_RAW", postdose)

        labsrc = row.get("LAB_SOURCE_RAW")
        if labsrc is not None and labsrc not in set(cfg["allowed"]["lab_sources"]):
            add_issue(issues, summary, rule_meta, "LB027", idx, row, "LAB_SOURCE_RAW", labsrc)
        if labsrc is not None and row.get("LAB_VENDOR_RAW") is None:
            add_issue(issues, summary, rule_meta, "LB028", idx, row, "LAB_VENDOR_RAW", row.get("LAB_VENDOR_RAW"))

    multi = df[["SUBJECT_KEY","VISIT_RAW","TEST_CODE_RAW","LAB_SOURCE_RAW"]].dropna().drop_duplicates()
    counts = multi.groupby(["SUBJECT_KEY","VISIT_RAW","TEST_CODE_RAW"])["LAB_SOURCE_RAW"].nunique()
    for (sk, vr, tc), n in counts.items():
        if n > 1:
            for idx, row in df[(df["SUBJECT_KEY"] == sk) & (df["VISIT_RAW"] == vr) & (df["TEST_CODE_RAW"] == tc)].iterrows():
                add_issue(issues, summary, rule_meta, "LB029", idx, row, "LAB_SOURCE_RAW", row.get("LAB_SOURCE_RAW"))

    catalog = cfg["test_catalog"]
    allowed_specimen = set(cfg["allowed"]["specimen"])
    allowed_cat = set(cfg["allowed"]["categorical_results"])
    allowed_qual = set(cfg["allowed"]["result_qualifiers"])

    for idx, row in df.iterrows():
        tc = row.get("TEST_CODE_RAW")
        tn = row.get("TEST_NAME_RAW")
        panel = row.get("TEST_PANEL_RAW")
        sp = row.get("SPECIMEN_RAW")
        result_raw = row.get("RESULT_RAW")
        result_char = row.get("RESULT_CHAR_RAW")
        result_num = row.get("RESULT_NUM_PARSED")
        if result_num is None:
            result_num = row.get("RESULT_RAW_PARSED")
        result_qual = row.get("RESULT_QUAL_RAW")
        nd = row.get("NOT_DONE_RAW")
        nd_reason = as_text(row.get("ND_REASON_RAW"))
        comment = as_text(row.get("COMMENT_RAW"))
        performed_like = (nd != "Y")
        any_result_field = any(present(x) for x in [result_raw, row.get("RESULT_NUM_RAW"), result_char])
        ancillary_present = any(present(x) for x in [row.get("ORIG_UNIT_RAW"), row.get("REF_LOW_RAW"), row.get("REF_HIGH_RAW"), row.get("ABN_FLAG_RAW")])

        if tc is None:
            add_issue(issues, summary, rule_meta, "LB030", idx, row, "TEST_CODE_RAW", tc)
        if tn is None:
            add_issue(issues, summary, rule_meta, "LB031", idx, row, "TEST_NAME_RAW", tn)

        if sp is None:
            add_issue(issues, summary, rule_meta, "LB032", idx, row, "SPECIMEN_RAW", sp)
        elif sp not in allowed_specimen:
            add_issue(issues, summary, rule_meta, "LB033", idx, row, "SPECIMEN_RAW", sp)

        entry = catalog.get(tc) if tc is not None else None
        numeric = None
        if entry is None and tc is not None:
            add_issue(issues, summary, rule_meta, "LB034", idx, row, "TEST_CODE_RAW", tc)
        elif entry is not None:
            numeric = bool(entry["numeric"])
            if tn is not None and tn not in set(entry["allowed_test_names"]):
                add_issue(issues, summary, rule_meta, "LB035", idx, row, "TEST_NAME_RAW", tn)
            if panel is not None and panel not in set(entry["panel"]):
                add_issue(issues, summary, rule_meta, "LB036", idx, row, "TEST_PANEL_RAW", panel)
            if sp is not None and sp in allowed_specimen and sp not in set(entry["specimen"]):
                add_issue(issues, summary, rule_meta, "LB037", idx, row, "SPECIMEN_RAW", sp)

        if performed_like and (not any_result_field) and (ancillary_present or tc is not None):
            add_issue(issues, summary, rule_meta, "LB038", idx, row, "RESULT_RAW", result_raw)

        allowed_yn = set(cfg["allowed"]["yn"])
        if nd is not None and nd not in allowed_yn:
            add_issue(issues, summary, rule_meta, "LB039", idx, row, "NOT_DONE_RAW", nd)

        hint_terms = ("CLOTTED", "NOT DONE", "QNS", "INSUFFICIENT", "CANCELLED", "SAMPLE NOT RECEIVED")
        if nd != "Y" and ((nd_reason != "") or any(term in comment for term in hint_terms)) and ((not any_result_field) or ancillary_present):
            add_issue(issues, summary, rule_meta, "LB040", idx, row, "NOT_DONE_RAW", nd)

        if nd == "Y":
            populated = any(present(x) for x in [
                result_raw, row.get("RESULT_NUM_RAW"), result_char,
                row.get("ORIG_UNIT_RAW"), row.get("REF_LOW_RAW"), row.get("REF_HIGH_RAW"),
                row.get("ABN_FLAG_RAW")
            ])
            if populated:
                add_issue(issues, summary, rule_meta, "LB041", idx, row, "NOT_DONE_RAW", nd)
            if nd_reason == "":
                add_issue(issues, summary, rule_meta, "LB042", idx, row, "ND_REASON_RAW", nd_reason)

        if nd != "Y" and nd_reason != "":
            add_issue(issues, summary, rule_meta, "LB043", idx, row, "ND_REASON_RAW", nd_reason)

        if numeric is False:
            if result_qual is not None and result_qual not in allowed_qual:
                add_issue(issues, summary, rule_meta, "LB044", idx, row, "RESULT_QUAL_RAW", result_qual)
            if result_raw is not None and result_raw in allowed_cat and result_char is None:
                add_issue(issues, summary, rule_meta, "LB045", idx, row, "RESULT_CHAR_RAW", result_char,
                          "Categorical result exists in RESULT_RAW but RESULT_CHAR_RAW is empty.")
            if result_raw in {"NEGATIVE","POSITIVE","TRACE"} and row.get("RESULT_NUM_RAW") is not None:
                add_issue(issues, summary, rule_meta, "LB046", idx, row, "RESULT_NUM_RAW", row.get("RESULT_NUM_RAW"),
                          "Categorical result also carries numeric encoding.")
        elif numeric is True:
            if present(result_raw) and (result_num is None) and (result_qual not in allowed_qual):
                add_issue(issues, summary, rule_meta, "LB047", idx, row, "RESULT_RAW", result_raw)

            unit = row.get("ORIG_UNIT_RAW")
            ref_unit = row.get("REF_UNIT_RAW")
            lo, hi = row.get("REF_LOW_NUM"), row.get("REF_HIGH_NUM")

            if performed_like:
                if not present(unit):
                    add_issue(issues, summary, rule_meta, "LB048", idx, row, "ORIG_UNIT_RAW", unit)
                elif entry is not None and unit not in set(entry["orig_units"]):
                    add_issue(issues, summary, rule_meta, "LB049", idx, row, "ORIG_UNIT_RAW", unit)

                if not present(ref_unit):
                    add_issue(issues, summary, rule_meta, "LB050", idx, row, "REF_UNIT_RAW", ref_unit)

                if present(unit) and present(ref_unit) and unit != ref_unit:
                    add_issue(issues, summary, rule_meta, "LB051", idx, row, "ORIG_UNIT_RAW/REF_UNIT_RAW", f"{unit} / {ref_unit}")

                if lo is None or hi is None:
                    add_issue(issues, summary, rule_meta, "LB052", idx, row, "REF_LOW_RAW/REF_HIGH_RAW", f"{row.get('REF_LOW_RAW')} / {row.get('REF_HIGH_RAW')}")
                elif lo >= hi:
                    add_issue(issues, summary, rule_meta, "LB053", idx, row, "REF_LOW_RAW/REF_HIGH_RAW", f"{row.get('REF_LOW_RAW')} / {row.get('REF_HIGH_RAW')}")

        abn = row.get("ABN_FLAG_RAW")
        if abn is not None and abn not in set(cfg["allowed"]["abn_flag"]):
            add_issue(issues, summary, rule_meta, "LB054", idx, row, "ABN_FLAG_RAW", abn)

        lo, hi = row.get("REF_LOW_NUM"), row.get("REF_HIGH_NUM")
        unit_ok_for_cmp = not (present(row.get("ORIG_UNIT_RAW")) and present(row.get("REF_UNIT_RAW")) and row.get("ORIG_UNIT_RAW") != row.get("REF_UNIT_RAW"))
        if numeric is True and result_num is not None and lo is not None and hi is not None and lo < hi and unit_ok_for_cmp:
            if not present(abn):
                add_issue(issues, summary, rule_meta, "LB055", idx, row, "ABN_FLAG_RAW", abn)
            else:
                is_low = result_num < lo
                is_high = result_num > hi
                is_normal = lo <= result_num <= hi
                if is_low and abn not in {"L","LOW"}:
                    add_issue(issues, summary, rule_meta, "LB055", idx, row, "ABN_FLAG_RAW", abn)
                elif is_high and abn not in {"H","HIGH"}:
                    add_issue(issues, summary, rule_meta, "LB055", idx, row, "ABN_FLAG_RAW", abn)
                elif is_normal and abn not in {"N","NORMAL"}:
                    add_issue(issues, summary, rule_meta, "LB055", idx, row, "ABN_FLAG_RAW", abn)

        cs = row.get("CLIN_SIG_RAW")
        if present(cs) and cs not in set(cfg["allowed"]["clin_sig"]):
            add_issue(issues, summary, rule_meta, "LB056", idx, row, "CLIN_SIG_RAW", cs)
        if cs == "Y" and (abn in {None, "N", "NORMAL", ""}):
            add_issue(issues, summary, rule_meta, "LB057", idx, row, "CLIN_SIG_RAW", cs)

        hem = row.get("HEMOLYZED_RAW")
        if present(hem) and hem not in set(cfg["allowed"]["hemolyzed_values"]):
            add_issue(issues, summary, rule_meta, "LB058", idx, row, "HEMOLYZED_RAW", hem)
        if hem == "Y" and any_result_field:
            add_issue(issues, summary, rule_meta, "LB059", idx, row, "HEMOLYZED_RAW", hem)

        rep = row.get("REPEAT_RAW")
        if present(rep) and rep not in set(cfg["allowed"]["repeat_values"]):
            add_issue(issues, summary, rule_meta, "LB060", idx, row, "REPEAT_RAW", rep)
        if ("REDRAW" in comment or "REPEAT" in comment or "RECOLLECT" in comment) and rep != "Y":
            add_issue(issues, summary, rule_meta, "LB061", idx, row, "REPEAT_RAW", rep, "COMMENT_RAW suggests redraw/repeat.")

    dup_mask = df.duplicated(subset=["SUBJECT_KEY","VISIT_RAW","TEST_CODE_RAW","COLL_DTM_RAW"], keep=False)
    for idx, row in df[dup_mask & df["TEST_CODE_RAW"].notna() & df["COLL_DTM_RAW"].notna()].iterrows():
        add_issue(issues, summary, rule_meta, "LB062", idx, row, "TEST_CODE_RAW/COLL_DTM_RAW", f"{row.get('TEST_CODE_RAW')} / {row.get('COLL_DTM_RAW')}")

    for idx, row in df[df["REPEAT_RAW"] == "Y"].iterrows():
        mask = (
            (df.index != idx) &
            (df["SUBJECT_KEY"] == row["SUBJECT_KEY"]) &
            (df["VISIT_RAW"] == row["VISIT_RAW"]) &
            (df["TEST_CODE_RAW"] == row["TEST_CODE_RAW"])
        )
        if not mask.any():
            add_issue(issues, summary, rule_meta, "LB063", idx, row, "REPEAT_RAW", row.get("REPEAT_RAW"))

    rng = df[["TEST_CODE_RAW","SPECIMEN_RAW","LAB_SOURCE_RAW","REF_UNIT_RAW","SEX_RAW","REF_LOW_RAW","REF_HIGH_RAW"]].dropna().drop_duplicates()
    counts = rng.groupby(["TEST_CODE_RAW","SPECIMEN_RAW","LAB_SOURCE_RAW","REF_UNIT_RAW","SEX_RAW"]).size()
    for key, n in counts.items():
        if n > 1:
            tc, sp, src, ru, sex = key
            mask = (
                (df["TEST_CODE_RAW"] == tc) &
                (df["SPECIMEN_RAW"] == sp) &
                (df["LAB_SOURCE_RAW"] == src) &
                (df["REF_UNIT_RAW"] == ru) &
                (df["SEX_RAW"] == sex)
            )
            for idx, row in df[mask].iterrows():
                add_issue(issues, summary, rule_meta, "LB064", idx, row, "REF_LOW_RAW/REF_HIGH_RAW", f"{row.get('REF_LOW_RAW')} / {row.get('REF_HIGH_RAW')}")

    issue_df = pd.DataFrame(issues)
    if issue_df.empty:
        issue_df = pd.DataFrame(columns=[
            "source_row_number","rule_id","severity","final_bucket","rule_description","classification_basis",
            "subject_key","visit_raw","visitnum_raw","test_code","test_name","variable_name","variable_value","message"
        ])

    issue_df = issue_df.sort_values(
        ["final_bucket","subject_key","visit_raw","test_code","source_row_number","rule_id"],
        na_position="last"
    ).reset_index(drop=True)

    summary_rows = []
    for (rule_id, severity, bucket, desc), count in sorted(summary.items()):
        meta = rule_meta[rule_id]
        summary_rows.append({
            "rule_id": rule_id,
            "severity": severity,
            "final_bucket": bucket,
            "rule_description": desc,
            "classification_basis": meta["basis"],
            "issue_count": count
        })
    summary_df = pd.DataFrame(summary_rows)

    df.to_csv(outdir / "lb_cleaned_output_v5.csv", index=False)
    issue_df.to_csv(outdir / "lb_issue_log_all_v5.csv", index=False)
    issue_df[issue_df["final_bucket"] == "Human"].to_csv(outdir / "lb_issue_log_human_v5.csv", index=False)
    issue_df[issue_df["final_bucket"] == "SDTM_STANDARDISABLE"].to_csv(outdir / "lb_issue_log_sdtm_standardisable_v5.csv", index=False)
    summary_df.to_csv(outdir / "lb_issue_summary_by_rule_v5.csv", index=False)

    print(f"Created outputs in: {outdir}")
    print(f"Input file used: {source}")

if __name__ == "__main__":
    main()
