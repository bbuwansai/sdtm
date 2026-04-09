from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Optional, Tuple, Dict

import pandas as pd

NULLS = {"", "NA", "NAN", "NONE", "NULL"}

RAW_REQUIRED_COLUMNS = [
    "PROTOCOL_NO","SITE_NO","SUBJECT_NO","SEX_RAW","AGE_YRS","LB_PAGE_ID","LB_LINE_NO",
    "LAB_SOURCE_RAW","LAB_VENDOR_RAW","FORM_NAME","VISIT_RAW","VISITNUM_RAW","UNSCHED_RAW",
    "VISITDT_RAW","COLL_DATE_RAW","COLL_TIME_RAW","COLL_DTM_RAW","FASTING_RAW","POSTDOSE_RAW",
    "SPECIMEN_RAW","TEST_PANEL_RAW","TEST_NAME_RAW","TEST_CODE_RAW","RESULT_RAW","RESULT_NUM_RAW",
    "RESULT_CHAR_RAW","RESULT_QUAL_RAW","ORIG_UNIT_RAW","REF_LOW_RAW","REF_HIGH_RAW",
    "REF_RANGE_TEXT_RAW","REF_UNIT_RAW","ABN_FLAG_RAW","CLIN_SIG_RAW","NOT_DONE_RAW","ND_REASON_RAW",
    "HEMOLYZED_RAW","REPEAT_RAW","SAMPLE_ID_RAW","COMMENT_RAW"
]

SPEC_FILES = {
    "spec": "lb_mapping_spec_validated_v4.csv",
    "test_map": "lb_test_map_v4.csv",
    "unit_map": "lb_unit_map_v4.csv",
    "visit_map": "lb_visit_map_v4.csv",
    "specimen_map": "lb_specimen_map_v4.csv",
}

LAYER1_FILES = {
    "human_issues": [
        "lb_issue_log_human_v5_1.csv",
        "lb_issue_log_human_v5.csv",
        "lb_issue_log_human_v4.csv",
    ],
    "sdtm_issues": [
        "lb_issue_log_sdtm_standardisable_v5_1.csv",
        "lb_issue_log_sdtm_standardisable_v5.csv",
        "lb_issue_log_sdtm_standardisable_v4.csv",
    ]
}

RAW_CANDIDATES = [
    "lb_raw_synthetic.csv",
    "lb_cleaned_output_v5_1.csv",
    "lb_cleaned_output_v5.csv",
    "lb_cleaned_output_v4.csv",
    "lb_cleaned_output.csv",
]

def clean(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    return None if s.upper() in NULLS else s

def uclean(v):
    s = clean(v)
    return s.upper() if isinstance(s, str) else s

def present(v) -> bool:
    return v is not None and not pd.isna(v)

def parse_num(v) -> Optional[float]:
    v = clean(v)
    if v is None:
        return None
    if re.fullmatch(r"-?\d+(?:\.\d+)?", str(v)):
        try:
            return float(v)
        except Exception:
            return None
    return None

def norm_text(v) -> Optional[str]:
    v = clean(v)
    return None if v is None else str(v).strip()

def parse_date_isoish(v) -> Tuple[Optional[str], str]:
    v = clean(v)
    if v is None:
        return None, "missing"
    s = str(v).strip()
    if re.fullmatch(r"\d{4}", s):
        return s, "iso_valid"
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s, "iso_valid"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            pd.to_datetime(s, format="%Y-%m-%d", errors="raise")
            return s, "iso_valid"
        except Exception:
            return None, "invalid"
    for fmt in ("%Y/%m/%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            return ts.strftime("%Y-%m-%d"), "alt_valid"
        except Exception:
            pass
    return None, "invalid"

def parse_time(v) -> Tuple[Optional[str], str]:
    v = clean(v)
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
            return f"{hh:02d}:{mm:02d}", "alt_valid"
        return None, "invalid"
    for fmt in ("%I:%M %p", "%H%M"):
        try:
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            return ts.strftime("%H:%M"), "alt_valid"
        except Exception:
            pass
    return None, "invalid"

def parse_dtm(v) -> Tuple[Optional[str], str]:
    v = clean(v)
    if v is None:
        return None, "missing"
    s = str(v).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?", s):
        try:
            if len(s) == 16:
                ts = pd.to_datetime(s, format="%Y-%m-%dT%H:%M", errors="raise")
                return ts.strftime("%Y-%m-%dT%H:%M"), "std_valid"
            ts = pd.to_datetime(s, format="%Y-%m-%dT%H:%M:%S", errors="raise")
            return ts.strftime("%Y-%m-%dT%H:%M:%S"), "std_valid"
        except Exception:
            return None, "invalid"
    for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M", "%m/%d/%Y %H:%M", "%d-%b-%Y %H:%M"):
        try:
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            return ts.strftime("%Y-%m-%dT%H:%M"), "alt_valid"
        except Exception:
            pass
    return None, "invalid"

def canonical_num_text(v) -> Optional[str]:
    n = parse_num(v)
    if n is None:
        return None
    return str(int(n)) if float(n).is_integer() else str(n)

def ensure_columns(df: pd.DataFrame, cols: List[str], label: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")

def load_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return pd.read_csv(path, dtype=str)

def summarize_issue_log(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["source_row_number", "issue_count", "rule_ids", "messages"])
    tmp = df.copy()
    tmp["source_row_number"] = pd.to_numeric(tmp["source_row_number"], errors="coerce")
    tmp = tmp.dropna(subset=["source_row_number"])
    tmp["source_row_number"] = tmp["source_row_number"].astype(int)
    return (
        tmp.groupby("source_row_number", dropna=False)
           .agg(
               issue_count=("rule_id", "count"),
               rule_ids=("rule_id", lambda s: " | ".join(sorted(pd.Series(s).dropna().astype(str).unique()))),
               messages=("message", lambda s: " || ".join(pd.Series(s).dropna().astype(str).unique()))
           )
           .reset_index()
    )

def file_score(path: Path, priority_names: List[str]) -> int:
    name = path.name.lower()
    score = 0
    for i, p in enumerate(priority_names):
        pl = p.lower()
        if name == pl:
            score += 1000 - i * 10
        elif pl in name:
            score += 200 - i * 5
    score += 5 if "output" not in str(path).lower() else 0
    score += 3 if "spec" in str(path).lower() else 0
    return score

def find_best_file(project_folder: Path, candidates: List[str]) -> Optional[Path]:
    all_files = [p for p in project_folder.rglob("*") if p.is_file()]
    matched = [p for p in all_files if any(c.lower() in p.name.lower() for c in candidates)]
    if not matched:
        return None
    matched = sorted(matched, key=lambda p: (file_score(p, candidates), str(p)), reverse=True)
    return matched[0]

def auto_detect_inputs(project_folder: Path) -> Dict[str, Path]:
    resolved = {}
    for key, exact_name in SPEC_FILES.items():
        p = find_best_file(project_folder, [exact_name])
        if p is None:
            raise FileNotFoundError(f"Could not auto-detect required spec file: {exact_name}")
        resolved[key] = p
    raw_path = find_best_file(project_folder, RAW_CANDIDATES)
    if raw_path is None:
        raise FileNotFoundError("Could not auto-detect raw LB file. Expected something like lb_raw_synthetic.csv")
    resolved["raw"] = raw_path
    for key, names in LAYER1_FILES.items():
        p = find_best_file(project_folder, names)
        if p is None:
            raise FileNotFoundError(f"Could not auto-detect Layer 1 file for {key}. Expected one of: {names}")
        resolved[key] = p
    return resolved

def build_lookup_tables(test_map: pd.DataFrame, unit_map: pd.DataFrame, visit_map: pd.DataFrame, specimen_map: pd.DataFrame):
    t = test_map.copy()
    for c in t.columns:
        t[c] = t[c].apply(uclean if c in {"raw_test_code","raw_test_name","lbcat","allowed_specimen","result_type"} else clean)
    code_name_lookup = {}
    code_lookup = {}
    for _, r in t.iterrows():
        code = r["raw_test_code"]
        name = r["raw_test_name"]
        standard_code = code if code != "PLAT" else "PLT"
        standard_name = name
        if name == "ALT (SGPT)":
            standard_name = "ALANINE AMINOTRANSFERASE"
        elif name == "AST (SGOT)":
            standard_name = "ASPARTATE AMINOTRANSFERASE"
        if present(code) and present(name):
            code_name_lookup[(code, name)] = {
                "LBTESTCD": standard_code,
                "LBTEST": standard_name,
                "LBCAT": r["lbcat"],
                "ALLOWED_SPECIMEN": set(str(r["allowed_specimen"]).split("|")) if present(r["allowed_specimen"]) else set(),
                "RESULT_TYPE": r["result_type"],
            }
        if present(standard_code):
            entry = code_lookup.setdefault(standard_code, {"names": set(), "lbcat": set(), "allowed_specimen": set(), "result_type": set()})
            if present(standard_name):
                entry["names"].add(standard_name)
            if present(r["lbcat"]):
                entry["lbcat"].add(r["lbcat"])
            if present(r["allowed_specimen"]):
                entry["allowed_specimen"] |= set(str(r["allowed_specimen"]).split("|"))
            if present(r["result_type"]):
                entry["result_type"].add(r["result_type"])

    u = unit_map.copy()
    for c in u.columns:
        u[c] = u[c].apply(uclean if c in {"lbtestcd","raw_unit","lborresu_normalized","lbstresu_standard","implemented_in_demo"} else clean)
    unit_lookup = {}
    for _, r in u.iterrows():
        unit_lookup[(r["lbtestcd"], r["raw_unit"])] = {
            "LBORRESU": r["lborresu_normalized"],
            "LBSTRESU": r["lbstresu_standard"],
            "CONVERSION_RULE": r["conversion_rule"],
            "IMPLEMENTED": (r["implemented_in_demo"] == "YES"),
        }

    v = visit_map.copy()
    for c in v.columns:
        v[c] = v[c].apply(uclean if c in {"visit_name","unsched_expected"} else clean)
    visit_lookup = {r["visit_name"]: {"VISITNUM": canonical_num_text(r["visitnum"]), "UNSCHED_EXPECTED": r["unsched_expected"], "NOTE": r.get("note")} for _, r in v.iterrows()}

    s = specimen_map.copy()
    for c in s.columns:
        s[c] = s[c].apply(uclean)
    specimen_lookup = {r["raw_specimen"]: r["lbspec"] for _, r in s.iterrows()}
    return code_name_lookup, code_lookup, unit_lookup, visit_lookup, specimen_lookup

def format_visitnum(v):
    if not present(v):
        return None
    try:
        f = float(v)
        return str(int(f)) if f.is_integer() else str(f)
    except:
        return str(v)

def apply_human_issue_gate(raw_df: pd.DataFrame, human_agg: pd.DataFrame):
    human_rows = set(human_agg["source_row_number"].tolist()) if not human_agg.empty else set()
    gated = raw_df[~raw_df["L1_SOURCE_ROW_NUMBER"].isin(human_rows)].copy()
    rejected = raw_df[raw_df["L1_SOURCE_ROW_NUMBER"].isin(human_rows)].copy()
    if not rejected.empty:
        rejected = rejected.merge(human_agg, how="left", left_on="L1_SOURCE_ROW_NUMBER", right_on="source_row_number")
        rejected["exception_type"] = "LAYER1_HUMAN"
    return gated, rejected

def map_visit(row, visit_lookup):
    visit = row.get("VISIT_RAW")
    raw_visitnum = canonical_num_text(row.get("VISITNUM_RAW"))
    if not present(visit):
        return visit, raw_visitnum, None, None
    info = visit_lookup.get(visit)
    if info is None:
        return visit, raw_visitnum, None, None
    return visit, raw_visitnum, info["VISITNUM"], info["UNSCHED_EXPECTED"]

def derive_lbdtc(row):
    reasons, autos = [], []
    dtm_norm, dtm_status = parse_dtm(row.get("COLL_DTM_RAW"))
    if dtm_status == "std_valid":
        return dtm_norm, reasons, autos
    if dtm_status == "alt_valid":
        autos.append("Standardized COLL_DTM_RAW to ISO 8601 for LBDTC")
        return dtm_norm, reasons, autos
    d_norm, _ = parse_date_isoish(row.get("COLL_DATE_RAW"))
    t_norm, _ = parse_time(row.get("COLL_TIME_RAW"))
    if d_norm and t_norm:
        if dtm_status == "missing":
            autos.append("Built LBDTC from COLL_DATE_RAW + COLL_TIME_RAW")
            return f"{d_norm}T{t_norm}", reasons, autos
        reasons.append("COLL_DTM_RAW invalid while COLL_DATE_RAW/COLL_TIME_RAW available")
        return None, reasons, autos
    reasons.append("Unable to derive valid LBDTC")
    return None, reasons, autos

def normalize_result_components(row, result_type):
    reasons, autos = [], []
    lborres = norm_text(row.get("RESULT_RAW"))
    result_num = parse_num(row.get("RESULT_NUM_RAW"))
    result_raw_num = parse_num(row.get("RESULT_RAW"))
    qual = norm_text(row.get("RESULT_QUAL_RAW"))
    result_char = norm_text(row.get("RESULT_CHAR_RAW"))
    if result_type == "CATEGORICAL":
        if not present(lborres) and present(result_char):
            lborres = result_char
            autos.append("Copied RESULT_CHAR_RAW into LBORRES for categorical result")
        if not present(lborres):
            reasons.append("Categorical test missing RESULT_RAW/RESULT_CHAR_RAW")
            return None, None, None, reasons, autos
        return lborres, lborres, None, reasons, autos
    if not present(lborres) and present(row.get("RESULT_NUM_RAW")):
        lborres = norm_text(row.get("RESULT_NUM_RAW"))
        autos.append("Copied RESULT_NUM_RAW into LBORRES for numeric result")
    if not present(lborres):
        reasons.append("Numeric test missing RESULT_RAW")
        return None, None, None, reasons, autos
    numeric_anchor = result_num if result_num is not None else result_raw_num
    if qual in {"<", ">", "<=", ">="}:
        autos.append("Preserved comparator result in LBORRES only")
        return lborres, None, None, reasons, autos
    if numeric_anchor is None:
        reasons.append("Numeric test lacks deterministic numeric value")
        return lborres, None, None, reasons, autos
    lbstresc = str(int(numeric_anchor)) if float(numeric_anchor).is_integer() else str(numeric_anchor)
    return lborres, lbstresc, numeric_anchor, reasons, autos

def map_test(row, code_name_lookup, code_lookup):
    reasons, autos = [], []
    raw_code = uclean(row.get("TEST_CODE_RAW"))
    raw_name = uclean(row.get("TEST_NAME_RAW"))
    if present(raw_code) and present(raw_name) and (raw_code, raw_name) in code_name_lookup:
        return code_name_lookup[(raw_code, raw_name)], reasons, autos
    canon_code = raw_code if raw_code != "PLAT" else "PLT"
    if present(raw_code) and raw_code == "PLAT":
        autos.append("Mapped raw test code PLAT to standard code family PLT")
    if present(canon_code) and canon_code in code_lookup:
        entry = code_lookup[canon_code]
        approved_names = sorted(entry["names"])
        if present(raw_name):
            if raw_name == "ALT (SGPT)":
                raw_name = "ALANINE AMINOTRANSFERASE"
            elif raw_name == "AST (SGOT)":
                raw_name = "ASPARTATE AMINOTRANSFERASE"
        if present(raw_name) and raw_name in entry["names"]:
            chosen_name = raw_name
        elif len(approved_names) == 1:
            chosen_name = approved_names[0]
            autos.append("Filled standard test name from single approved code mapping")
        else:
            reasons.append("Multiple approved test names for code but no deterministic raw-name resolution")
            return None, reasons, autos
        lbcat = sorted(entry["lbcat"])[0] if len(entry["lbcat"]) == 1 else None
        result_type = sorted(entry["result_type"])[0] if len(entry["result_type"]) == 1 else None
        if lbcat is None or result_type is None:
            reasons.append("Test code mapping is not deterministic")
            return None, reasons, autos
        return {"LBTESTCD": canon_code, "LBTEST": chosen_name, "LBCAT": lbcat, "ALLOWED_SPECIMEN": entry["allowed_specimen"], "RESULT_TYPE": result_type}, reasons, autos
    reasons.append("Unmapped TEST_CODE_RAW / TEST_NAME_RAW")
    return None, reasons, autos

def standardize_unit(lbtestcd, raw_unit, unit_lookup):
    if not present(lbtestcd) or not present(raw_unit):
        return None
    return unit_lookup.get((uclean(lbtestcd), uclean(raw_unit)))

def derive_lbnrind(raw_abn, numeric_value, low, high, unit_context_ok):
    raw_abn = uclean(raw_abn)
    if present(raw_abn):
        mapping = {"N": "NORMAL", "NORMAL": "NORMAL", "L": "LOW", "LOW": "LOW", "H": "HIGH", "HIGH": "HIGH"}
        return mapping.get(raw_abn)
    if numeric_value is None or low is None or high is None or not unit_context_ok:
        return None
    if numeric_value < low:
        return "LOW"
    if numeric_value > high:
        return "HIGH"
    return "NORMAL"

def rows_equal_for_final(group_df: pd.DataFrame, final_cols: List[str]) -> bool:
    if len(group_df) <= 1:
        return True
    subset = group_df[final_cols].fillna("")
    first = subset.iloc[0].tolist()
    return all(row.tolist() == first for _, row in subset.iloc[1:].iterrows())

def generate_lb(project_folder: Path, outdir: Path, keep_not_done_rows: str = "Y"):
    resolved = auto_detect_inputs(project_folder)

    raw = load_csv(resolved["raw"], "Raw LB")
    spec = load_csv(resolved["spec"], "LB spec")
    test_map = load_csv(resolved["test_map"], "LB test map")
    unit_map = load_csv(resolved["unit_map"], "LB unit map")
    visit_map = load_csv(resolved["visit_map"], "LB visit map")
    specimen_map = load_csv(resolved["specimen_map"], "LB specimen map")
    human_log = load_csv(resolved["human_issues"], "Human issue log")
    sdtm_log = load_csv(resolved["sdtm_issues"], "SDTM-standardisable issue log")

    ensure_columns(raw, RAW_REQUIRED_COLUMNS, "Raw LB")
    ensure_columns(spec, ["target_variable"], "LB spec")
    ensure_columns(human_log, ["source_row_number", "rule_id", "message"], "Human issue log")
    ensure_columns(sdtm_log, ["source_row_number", "rule_id", "message"], "SDTM-standardisable issue log")

    if "L1_SOURCE_ROW_NUMBER" not in raw.columns:
        raw.insert(0, "L1_SOURCE_ROW_NUMBER", range(1, len(raw) + 1))

    for c in raw.columns:
        raw[c] = raw[c].apply(clean)
    upper_cols = [
        "PROTOCOL_NO","SITE_NO","SUBJECT_NO","SEX_RAW","LAB_SOURCE_RAW","LAB_VENDOR_RAW","FORM_NAME",
        "VISIT_RAW","VISITNUM_RAW","UNSCHED_RAW","FASTING_RAW","POSTDOSE_RAW","SPECIMEN_RAW","TEST_PANEL_RAW",
        "TEST_NAME_RAW","TEST_CODE_RAW","RESULT_RAW","RESULT_NUM_RAW","RESULT_CHAR_RAW","RESULT_QUAL_RAW",
        "ORIG_UNIT_RAW","REF_RANGE_TEXT_RAW","REF_UNIT_RAW","ABN_FLAG_RAW","CLIN_SIG_RAW","NOT_DONE_RAW",
        "ND_REASON_RAW","HEMOLYZED_RAW","REPEAT_RAW","SAMPLE_ID_RAW","COMMENT_RAW"
    ]
    for c in upper_cols:
        raw[c] = raw[c].apply(uclean)
    raw["L1_SOURCE_ROW_NUMBER"] = pd.to_numeric(raw["L1_SOURCE_ROW_NUMBER"], errors="coerce").astype(int)

    human_agg = summarize_issue_log(human_log)
    sdtm_agg = summarize_issue_log(sdtm_log)
    candidate_raw, human_rejected = apply_human_issue_gate(raw, human_agg)
    human_rejected.to_csv(outdir / "lb_exceptions_human.csv", index=False)

    code_name_lookup, code_lookup, unit_lookup, visit_lookup, specimen_lookup = build_lookup_tables(test_map, unit_map, visit_map, specimen_map)
    spec_target_order = spec["target_variable"].dropna().astype(str).tolist()
    final_rows, transform_exceptions, auto_logs = [], [], []
    sdtm_issue_rows = set(sdtm_agg["source_row_number"].tolist()) if not sdtm_agg.empty else set()

    for _, row in candidate_raw.iterrows():
        src_row = int(row["L1_SOURCE_ROW_NUMBER"])
        reasons, autos = [], []

        mapped_test, r1, a1 = map_test(row, code_name_lookup, code_lookup)
        reasons += r1; autos += a1
        if mapped_test is None:
            transform_exceptions.append({"source_row_number": src_row, "exception_type": "TRANSFORM_UNRESOLVED", "reason": " | ".join(sorted(set(reasons))), **row.to_dict()})
            continue

        lbtestcd = mapped_test["LBTESTCD"]; lbtest = mapped_test["LBTEST"]; lbcat = mapped_test["LBCAT"]; result_type = mapped_test["RESULT_TYPE"]

        raw_spec = row.get("SPECIMEN_RAW")
        lbspec = specimen_lookup.get(raw_spec) if present(raw_spec) else None
        if present(raw_spec) and lbspec is None:
            reasons.append("Unmapped specimen")
        if present(lbspec) and lbspec not in mapped_test["ALLOWED_SPECIMEN"]:
            reasons.append("Specimen incompatible with mapped test")

        visit, raw_visitnum, expected_visitnum, expected_uns = map_visit(row, visit_lookup)
        if present(visit) and present(expected_visitnum) and present(raw_visitnum) and raw_visitnum != expected_visitnum:
            reasons.append("VISIT_RAW and VISITNUM_RAW contradiction")
        if present(row.get("UNSCHED_RAW")) and present(expected_uns) and uclean(row.get("UNSCHED_RAW")) != expected_uns and "UNSCHED" in str(visit or ""):
            reasons.append("VISIT_RAW and UNSCHED_RAW contradiction")

        lbdtc, r2, a2 = derive_lbdtc(row)
        reasons += r2; autos += a2

        studyid = norm_text(row.get("PROTOCOL_NO"))
        site = norm_text(row.get("SITE_NO"))
        subj = norm_text(row.get("SUBJECT_NO"))
        usubjid = f"{studyid}-{site}-{subj}" if all([present(studyid), present(site), present(subj)]) else None
        if not present(usubjid):
            reasons.append("Cannot derive USUBJID")

        lbfast = uclean(row.get("FASTING_RAW"))
        if lbfast not in {None, "Y", "N", "U"}:
            reasons.append("Invalid LBFAST source value")

        not_done = uclean(row.get("NOT_DONE_RAW"))
        lbstat = "NOT DONE" if not_done == "Y" else None
        lbreasnd = norm_text(row.get("ND_REASON_RAW")) if not_done == "Y" else None

        lborres, lbstresc, numeric_anchor, r3, a3 = normalize_result_components(row, result_type)
        reasons += r3; autos += a3

        if not_done == "Y":
            if keep_not_done_rows == "N":
                transform_exceptions.append({"source_row_number": src_row, "exception_type": "NOT_DONE_EXCLUDED_BY_POLICY", "reason": "Sponsor policy excludes NOT DONE rows from final LB", **row.to_dict()})
                continue
            lborres = None; lbstresc = None; numeric_anchor = None

        raw_unit = uclean(row.get("ORIG_UNIT_RAW"))
        raw_ref_unit = uclean(row.get("REF_UNIT_RAW"))
        unit_meta = standardize_unit(lbtestcd, raw_unit, unit_lookup) if present(raw_unit) else None
        lborresu = unit_meta["LBORRESU"] if unit_meta else None
        lbstresu = unit_meta["LBSTRESU"] if unit_meta and unit_meta["IMPLEMENTED"] else None
        if present(raw_unit) and unit_meta is None and result_type == "NUMERIC" and not_done != "Y":
            reasons.append("Unsupported original unit for numeric test")

        lbstresn = None
        if numeric_anchor is not None:
            if unit_meta and unit_meta["IMPLEMENTED"]:
                lbstresn = numeric_anchor
                autos.append("Carried numeric result forward without unit conversion" if unit_meta["CONVERSION_RULE"] == "no conversion" else "Applied implemented standard-unit conversion")
            elif result_type == "NUMERIC" and not_done != "Y":
                reasons.append("Cannot derive LBSTRESN deterministically")

        lbornrlo = parse_num(row.get("REF_LOW_RAW"))
        lbornrhi = parse_num(row.get("REF_HIGH_RAW"))
        if lbornrlo is not None and lbornrhi is not None and lbornrlo >= lbornrhi:
            reasons.append("Original reference range order invalid")
            lbornrlo = None; lbornrhi = None

        unit_context_ok = (not present(raw_ref_unit)) or (not present(raw_unit)) or (raw_ref_unit == raw_unit)
        lbstnrlo = None; lbstnrhi = None
        if lbornrlo is not None and lbornrhi is not None and unit_context_ok and lbstresu is not None:
            lbstnrlo = lbornrlo; lbstnrhi = lbornrhi
        elif (parse_num(row.get("REF_LOW_RAW")) is not None or parse_num(row.get("REF_HIGH_RAW")) is not None) and not unit_context_ok:
            reasons.append("Reference range unit context incompatible with standardization")

        lbnrind = derive_lbnrind(row.get("ABN_FLAG_RAW"), lbstresn, lbstnrlo, lbstnrhi, unit_context_ok)

        if not present(studyid):
            reasons.append("Missing STUDYID source")
        if not present(lbtestcd) or not present(lbtest):
            reasons.append("Missing mapped test metadata")
        if not_done != "Y":
            if not present(lbdtc):
                reasons.append("Missing required LBDTC for performed row")
            if not present(lborres):
                reasons.append("Missing required LBORRES for performed row")

        if reasons:
            transform_exceptions.append({"source_row_number": src_row, "exception_type": "TRANSFORM_UNRESOLVED", "reason": " | ".join(sorted(set(reasons))), **row.to_dict()})
            continue

        final_rows.append({
            "STUDYID": studyid, "DOMAIN": "LB", "USUBJID": usubjid, "LBSEQ": None,
            "LBTESTCD": lbtestcd, "LBTEST": lbtest, "LBCAT": lbcat, "LBSPEC": lbspec,
            "LBLOINC": None, "LBORRES": lborres, "LBORRESU": lborresu, "LBORNRLO": lbornrlo,
            "LBORNRHI": lbornrhi, "LBSTRESC": lbstresc, "LBSTRESN": lbstresn, "LBSTRESU": lbstresu,
            "LBSTNRLO": lbstnrlo, "LBSTNRHI": lbstnrhi, "LBNRIND": lbnrind, "LBDTC": lbdtc,
            "VISITNUM": format_visitnum(raw_visitnum),
            "LBFAST": lbfast, "LBSTAT": lbstat, "LBREASND": lbreasnd, "LBMETHOD": None,
            "_SOURCE_ROW_NUMBER": src_row, "_RAW_LAB_SOURCE": row.get("LAB_SOURCE_RAW"),
            "_RAW_REPEAT": row.get("REPEAT_RAW"), "_RAW_COMMENT": row.get("COMMENT_RAW"),
        })

        if autos or src_row in sdtm_issue_rows:
            auto_logs.append({"source_row_number": src_row, "auto_actions": " | ".join(autos) if autos else "Row had Layer 1 SDTM-standardisable issue(s); deterministic transform completed."})

    final_df = pd.DataFrame(final_rows)
    exc_df = pd.DataFrame(transform_exceptions)
    auto_df = pd.DataFrame(auto_logs)
    exact_dups_dropped = pd.DataFrame()

    if not final_df.empty:
        final_df = final_df.sort_values(["STUDYID", "USUBJID", "LBTESTCD", "LBDTC", "VISITNUM", "_SOURCE_ROW_NUMBER"], na_position="last").reset_index(drop=True)
        final_cols_nohelpers = [c for c in final_df.columns if not c.startswith("_") and c != "LBSEQ"]
        group_key = ["STUDYID", "USUBJID", "LBTESTCD", "LBDTC", "VISITNUM"]

        keep_indices, drop_rows, route_rows = [], [], []
        for _, grp in final_df.groupby(group_key, dropna=False, sort=False):
            if len(grp) == 1:
                keep_indices.extend(grp.index.tolist())
                continue
            if rows_equal_for_final(grp, final_cols_nohelpers):
                keep_indices.append(grp.index[0])
                if len(grp) > 1:
                    dropped = grp.iloc[1:].copy()
                    dropped["drop_reason"] = "Exact duplicate of retained final LB row"
                    drop_rows.append(dropped)
                continue
            routed = grp.copy()
            routed["exception_type"] = "ROW_SELECTION_CONFLICT"
            routed["reason"] = "Conflicting duplicate/repeat/multi-source final-candidate rows require manual resolution"
            route_rows.append(routed)

        kept_df = final_df.loc[sorted(set(keep_indices))].copy() if keep_indices else pd.DataFrame(columns=final_df.columns)
        if drop_rows:
            exact_dups_dropped = pd.concat(drop_rows, ignore_index=True)
        if route_rows:
            routed_df = pd.concat(route_rows, ignore_index=True)
            routed_src = set(routed_df["_SOURCE_ROW_NUMBER"].tolist())
            kept_df = kept_df[~kept_df["_SOURCE_ROW_NUMBER"].isin(routed_src)].copy()
            exc_df = pd.concat([exc_df, routed_df], ignore_index=True, sort=False)
        final_df = kept_df.reset_index(drop=True)
        final_df["LBSEQ"] = final_df.groupby("USUBJID", dropna=False).cumcount() + 1

    output_cols = [c for c in spec_target_order if c in final_df.columns]
    final_ordered = final_df[output_cols].copy() if not final_df.empty else pd.DataFrame(columns=output_cols)

    build_summary = pd.DataFrame([
        {"metric": "raw_input_rows", "value": len(raw)},
        {"metric": "rows_rejected_by_layer1_human", "value": 0 if human_rejected.empty else len(human_rejected)},
        {"metric": "rows_after_human_gate", "value": len(candidate_raw)},
        {"metric": "rows_routed_to_transform_exceptions", "value": 0 if exc_df.empty else len(exc_df)},
        {"metric": "rows_dropped_as_exact_duplicates", "value": 0 if exact_dups_dropped.empty else len(exact_dups_dropped)},
        {"metric": "final_lb_rows", "value": len(final_ordered)},
        {"metric": "rows_auto_standardized_logged", "value": 0 if auto_df.empty else len(auto_df)},
    ])

    final_df.to_csv(outdir / "lb_final_sdtm.csv", index=False)
    final_ordered.to_csv(outdir / "lb_final_sdtm_ordered.csv", index=False)
    exc_df.to_csv(outdir / "lb_exceptions_transform.csv", index=False)
    exact_dups_dropped.to_csv(outdir / "lb_exact_duplicates_dropped.csv", index=False)
    auto_df.to_csv(outdir / "lb_auto_standardized_log.csv", index=False)
    build_summary.to_csv(outdir / "lb_build_summary.csv", index=False)
    pd.DataFrame([{"file_role": k, "path": str(v)} for k, v in resolved.items()]).to_csv(outdir / "lb_detected_inputs.csv", index=False)

    print(f"Created outputs in: {outdir}")
    print("Detected inputs:")
    for k, v in resolved.items():
        print(f"  {k}: {v}")

def main():
    parser = argparse.ArgumentParser(description="Fast auto-detecting LB SDTM generator")
    parser.add_argument("--project-folder", default=".", help="Folder containing raw/spec/layer1 files")
    parser.add_argument("--outdir", default=None, help="Optional output folder. Default: <project-folder>/lb_sdtm_output_v2")
    parser.add_argument("--keep-not-done-rows", choices=["Y", "N"], default="Y")
    args = parser.parse_args()

    project_folder = Path(args.project_folder).resolve()
    outdir = Path(args.outdir).resolve() if args.outdir else (project_folder / "lb_sdtm_output_v2")
    outdir.mkdir(parents=True, exist_ok=True)
    generate_lb(project_folder=project_folder, outdir=outdir, keep_not_done_rows=args.keep_not_done_rows)

if __name__ == "__main__":
    main()
