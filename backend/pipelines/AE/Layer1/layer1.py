import argparse
import json
import re
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd

NULLS = {"", "NA", "NAN", "NONE", "NULL"}
EXPECTED_COLUMNS = [
    "ROW_ID","STUDYID_RAW","SITEID_RAW","SUBJECT_RAW","SCREENING_NO","RAND_NO","VISIT_RAW","VISITDT_RAW",
    "AE_FORM_SEQ","AE_SEQ_CRf","AEYN_RAW","AE_TERM","AE_START_DATE_RAW","AE_START_TIME_RAW","AE_END_DATE_RAW",
    "AE_END_TIME_RAW","AE_ONGOING_RAW","AE_SEVERITY_RAW","AE_TOXGR_RAW","AE_SER_RAW","AE_SER_DTH_RAW",
    "AE_SER_LIFE_RAW","AE_SER_HOSP_RAW","AE_SER_DISAB_RAW","AE_SER_CONG_RAW","AE_SER_MIE_RAW",
    "AE_REL_STUDY_DRUG_RAW","AE_REL_STUDY_DRUG2_RAW","AE_ACTION_DRUG_RAW","AE_ACTION_DRUG2_RAW",
    "AE_ACTION_OTHER_TXT","AE_OUTCOME_RAW","AE_PRESPEC_RAW","AE_REPORTED_BY","AE_REPORT_DATE_RAW",
    "AE_COMMENT","ENTRY_STATUS_RAW","CHANGE_REASON_RAW"
]

TEXT_COLUMNS = [
    "ROW_ID","STUDYID_RAW","SITEID_RAW","SUBJECT_RAW","SCREENING_NO","RAND_NO","VISIT_RAW","AE_FORM_SEQ","AE_SEQ_CRf",
    "AEYN_RAW","AE_TERM","AE_ONGOING_RAW","AE_SEVERITY_RAW","AE_TOXGR_RAW","AE_SER_RAW","AE_SER_DTH_RAW",
    "AE_SER_LIFE_RAW","AE_SER_HOSP_RAW","AE_SER_DISAB_RAW","AE_SER_CONG_RAW","AE_SER_MIE_RAW",
    "AE_REL_STUDY_DRUG_RAW","AE_REL_STUDY_DRUG2_RAW","AE_ACTION_DRUG_RAW","AE_ACTION_DRUG2_RAW",
    "AE_ACTION_OTHER_TXT","AE_OUTCOME_RAW","AE_PRESPEC_RAW","AE_REPORTED_BY","AE_COMMENT","ENTRY_STATUS_RAW",
    "CHANGE_REASON_RAW"
]
DATE_COLUMNS = ["VISITDT_RAW","AE_START_DATE_RAW","AE_END_DATE_RAW","AE_REPORT_DATE_RAW"]
TIME_COLUMNS = ["AE_START_TIME_RAW","AE_END_TIME_RAW"]


def clean(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    return None if s.upper() in NULLS else s


def upper_clean(v):
    s = clean(v)
    return s.upper() if isinstance(s, str) else s


def raw_text(v):
    if pd.isna(v):
        return None
    return str(v)


def present(v):
    return v is not None and not pd.isna(v) and str(v).strip() != ""


def as_text(v):
    if pd.isna(v):
        return ""
    return str(v).strip()


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
        return iso, "partial" if len(iso) < 10 else "iso_valid"
    if re.fullmatch(r"UNK/\d{2}/\d{4}", s.upper()):
        return None, "invalid"
    for fmt in ("%Y/%m/%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            ts = pd.to_datetime(s, format=fmt, errors="raise")
            return ts.strftime("%Y-%m-%d"), "alt_valid"
        except Exception:
            pass
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?", s):
        try:
            ts = pd.to_datetime(s, errors="raise")
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


def as_yes_no(v):
    s = upper_clean(v)
    if s in {"Y", "YES"}:
        return "YES"
    if s in {"N", "NO"}:
        return "NO"
    return s


def first_existing(base: Path, names: List[str]) -> Optional[Path]:
    for name in names:
        p = base / name
        if p.exists():
            return p
    return None


def autodetect_source(search_dir: Path, expected_name: str) -> Path:
    direct = search_dir / expected_name
    if direct.exists():
        return direct
    candidates = sorted(search_dir.glob("*ae*raw*.csv")) + sorted(search_dir.glob("*AE*raw*.csv")) + sorted(search_dir.glob("*ae*.csv"))
    candidates = list(dict.fromkeys(candidates))
    if len(candidates) == 1:
        return candidates[0]
    exactish = [p for p in candidates if "synthetic" in p.name.lower()]
    if len(exactish) == 1:
        return exactish[0]
    raise FileNotFoundError(
        f"Could not uniquely auto-detect AE source CSV in {search_dir}. Expected {expected_name} or a single AE-like CSV."
    )


def main():
    parser = argparse.ArgumentParser(description="AE Layer 1 QC v7 (JSON-driven, raw-friendly terminology checks, de-duplicated, sorted by row)")
    parser.add_argument("--source", help="Optional AE raw CSV path")
    parser.add_argument("--rules", help="Optional AE rules JSON path")
    parser.add_argument("--outdir", help="Optional output directory override")
    parser.add_argument("--search-dir", help="Optional directory for auto-detecting files")
    args = parser.parse_args()

    base = Path(__file__).resolve().parent
    search_dir = Path(args.search_dir) if args.search_dir else base

    rules_path = Path(args.rules) if args.rules else first_existing(search_dir, ["ae_layer1_rules_v7.json", "ae_layer1_rules_v6.json", "ae_layer1_rules_v5.json", "ae_layer1_rules_v4.json", "ae_layer1_rules.json"])
    if not rules_path or not rules_path.exists():
        raise FileNotFoundError(f"Rules JSON not found in {search_dir}.")
    cfg = json.loads(rules_path.read_text(encoding="utf-8"))

    source = Path(args.source) if args.source else autodetect_source(search_dir, cfg["input"]["source_csv"])
    if not source.exists():
        raise FileNotFoundError(f"Input CSV not found: {source}")

    outdir = Path(args.outdir) if args.outdir else (search_dir / "ae_layer1_outputs_v7")
    outdir.mkdir(parents=True, exist_ok=True)

    raw_df = pd.read_csv(source, dtype=str)
    missing_cols = [c for c in EXPECTED_COLUMNS if c not in raw_df.columns]
    if missing_cols:
        raise ValueError("Input file is missing required columns: " + ", ".join(missing_cols))

    df = raw_df.copy()
    if "L1_SOURCE_ROW_NUMBER" not in df.columns:
        df.insert(0, "L1_SOURCE_ROW_NUMBER", range(1, len(df) + 1))

    for c in TEXT_COLUMNS + DATE_COLUMNS + TIME_COLUMNS:
        df[f"RAW__{c}"] = df[c].apply(raw_text)

    for c in df.columns:
        if not c.startswith("RAW__"):
            df[c] = df[c].apply(clean)

    upper_cols = [
        "STUDYID_RAW","SITEID_RAW","SUBJECT_RAW","SCREENING_NO","RAND_NO","VISIT_RAW","AEYN_RAW","AE_TERM",
        "AE_ONGOING_RAW","AE_SEVERITY_RAW","AE_TOXGR_RAW","AE_SER_RAW","AE_SER_DTH_RAW","AE_SER_LIFE_RAW",
        "AE_SER_HOSP_RAW","AE_SER_DISAB_RAW","AE_SER_CONG_RAW","AE_SER_MIE_RAW","AE_REL_STUDY_DRUG_RAW",
        "AE_REL_STUDY_DRUG2_RAW","AE_ACTION_DRUG_RAW","AE_ACTION_DRUG2_RAW","AE_ACTION_OTHER_TXT",
        "AE_OUTCOME_RAW","AE_PRESPEC_RAW","AE_REPORTED_BY","AE_COMMENT","ENTRY_STATUS_RAW","CHANGE_REASON_RAW"
    ]
    for col in upper_cols:
        df[col] = df[col].apply(upper_clean)

    yn_cols = ["AEYN_RAW","AE_ONGOING_RAW","AE_SER_RAW","AE_SER_DTH_RAW","AE_SER_LIFE_RAW","AE_SER_HOSP_RAW","AE_SER_DISAB_RAW","AE_SER_CONG_RAW","AE_SER_MIE_RAW","AE_PRESPEC_RAW"]
    for col in yn_cols:
        df[f"CANON__{col}"] = df[col].apply(as_yes_no)

    # Raw-friendly canonicalization for common sponsor CT variants
    outcome_map = {
        "RECOVERED": "RECOVERED/RESOLVED",
        "RESOLVED": "RECOVERED/RESOLVED",
        "RECOVERED/RESOLVED": "RECOVERED/RESOLVED",
        "RECOVERING": "RECOVERING/RESOLVING",
        "RESOLVING": "RECOVERING/RESOLVING",
        "RECOVERING/RESOLVING": "RECOVERING/RESOLVING",
        "NOT RECOVERED": "NOT RECOVERED/NOT RESOLVED",
        "NOT RESOLVED": "NOT RECOVERED/NOT RESOLVED",
        "NOT RECOVERED/NOT RESOLVED": "NOT RECOVERED/NOT RESOLVED",
        "FATAL": "FATAL",
        "UNKNOWN": "UNKNOWN",
    }
    reporter_map = {
        "INVESTIGATOR": "INVESTIGATOR",
        "SUB-INVESTIGATOR": "SUB-INVESTIGATOR",
        "SUB INVESTIGATOR": "SUB-INVESTIGATOR",
        "COORDINATOR": "STUDY COORDINATOR",
        "STUDY COORDINATOR": "STUDY COORDINATOR",
        "SUBJECT": "SUBJECT",
    }
    entry_status_map = {
        "COMPLETE": "COMPLETE",
        "UPDATED": "UPDATED",
        "INITIAL": "COMPLETE",
        "VERIFIED": "COMPLETE",
    }

    df["CANON__AE_OUTCOME_RAW"] = df["AE_OUTCOME_RAW"].map(lambda x: outcome_map.get(x, x))
    df["CANON__AE_REPORTED_BY"] = df["AE_REPORTED_BY"].map(lambda x: reporter_map.get(x, x))
    df["CANON__ENTRY_STATUS_RAW"] = df["ENTRY_STATUS_RAW"].map(lambda x: entry_status_map.get(x, x))

    df["SUBJECT_KEY"] = df[["STUDYID_RAW","SITEID_RAW","SUBJECT_RAW"]].fillna("").agg("|".join, axis=1)
    for col in DATE_COLUMNS:
        df[[f"{col}_NORM", f"{col}_STATUS"]] = df[col].apply(lambda x: pd.Series(parse_date_value(x)))
    for col in TIME_COLUMNS:
        df[[f"{col}_NORM", f"{col}_STATUS"]] = df[col].apply(lambda x: pd.Series(parse_time_value(x)))

    rule_meta = cfg["rules"]
    suppression_cfg = cfg.get("suppression", {})
    suppress_by_rule = suppression_cfg.get("by_rule", {})
    priority_map = suppression_cfg.get("priority", {})

    issues = []
    summary = {}
    issue_keys = set()
    row_rule_hits = {}

    def normalize_focus_column(col: Optional[str]) -> str:
        if not col:
            return ""
        return "/".join(sorted(set([c.strip() for c in str(col).split("/") if c.strip()])))

    def root_key_for(row_idx, focus_column):
        return (int(row_idx), normalize_focus_column(focus_column))

    def has_rule_on_root(row_idx, focus_column, rule_ids):
        hits = row_rule_hits.get(root_key_for(row_idx, focus_column), [])
        hit_ids = {r["rule_id"] for r in hits}
        return any(rid in hit_ids for rid in rule_ids)

    def best_hit_for_root(row_idx, focus_column):
        hits = row_rule_hits.get(root_key_for(row_idx, focus_column), [])
        if not hits:
            return None
        return sorted(hits, key=lambda r: (priority_map.get(r["rule_id"], 999), r["rule_id"]))[0]

    def add_issue(rule_id, row_idx=None, row=None, focus_column=None, focus_value=None, message_override=None):
        src_row = None if row_idx is None else int(row.get("L1_SOURCE_ROW_NUMBER", row_idx + 1))
        key = (src_row, rule_id, normalize_focus_column(focus_column), str(focus_value), str(message_override or ""))
        if key in issue_keys:
            return
        issue_keys.add(key)
        meta = rule_meta[rule_id]
        rec = {
            "source_row_number": src_row,
            "rule_id": rule_id,
            "severity": meta["severity"],
            "final_bucket": meta["bucket"],
            "rule_description": meta["description"],
            "classification_basis": meta["basis"],
            "subject_key": None if row is None else row.get("SUBJECT_KEY"),
            "visit_raw": None if row is None else row.get("VISIT_RAW"),
            "visitnum_raw": None,
            "test_code": None,
            "test_name": None if row is None else row.get("AE_TERM"),
            "variable_name": focus_column,
            "variable_value": focus_value,
            "message": message_override or meta["description"],
        }
        issues.append(rec)
        root = root_key_for(row_idx if row_idx is not None else -1, focus_column)
        row_rule_hits.setdefault(root, []).append(rec)

    def has_whitespace_issue(raw_value):
        if raw_value is None:
            return False
        return str(raw_value) != "" and str(raw_value) != str(raw_value).strip()

    def row_numbers_as_text(indexes, current_idx):
        nums = sorted(int(df.loc[i, "L1_SOURCE_ROW_NUMBER"]) for i in indexes if int(df.loc[i, "L1_SOURCE_ROW_NUMBER"]) != int(df.loc[current_idx, "L1_SOURCE_ROW_NUMBER"]))
        return ", ".join(str(n) for n in nums)

    # Required fields
    for idx, row in df.iterrows():
        for col in cfg["required_fields_always"]:
            if row.get(col) is None:
                add_issue("AE001", idx, row, col, None, f"{col} is required at Layer 1.")

    # Dataset-level consistency
    if df["STUDYID_RAW"].dropna().nunique() > 1:
        for idx, row in df.iterrows():
            add_issue("AE003", idx, row, "STUDYID_RAW", row.get("STUDYID_RAW"))

    parsed_site = df["SUBJECT_RAW"].apply(lambda x: str(x).split("-")[0] if present(x) and "-" in str(x) else None)
    bad_site_mask = parsed_site.notna() & df["SITEID_RAW"].notna() & (parsed_site != df["SITEID_RAW"])
    for idx, row in df[bad_site_mask].iterrows():
        add_issue("AE004", idx, row, "SUBJECT_RAW", row.get("SUBJECT_RAW"), "SUBJECT_RAW prefix does not match SITEID_RAW.")

    ae_detail_cols = [
        "AE_TERM","AE_START_DATE_RAW","AE_START_TIME_RAW","AE_END_DATE_RAW","AE_END_TIME_RAW",
        "AE_ONGOING_RAW","AE_SEVERITY_RAW","AE_TOXGR_RAW","AE_SER_RAW","AE_OUTCOME_RAW",
        "AE_REL_STUDY_DRUG_RAW","AE_COMMENT"
    ]
    df["AE_PRESENT_ROW"] = [
        (row.get("CANON__AEYN_RAW") == "YES") or any(present(row.get(c)) for c in ae_detail_cols)
        for _, row in df.iterrows()
    ]

    allowed_yn = set(cfg["allowed"]["yes_no"])
    for idx, row in df.iterrows():
        canon = row.get("CANON__AEYN_RAW")
        raw = row.get("AEYN_RAW")
        if raw is not None and canon not in allowed_yn:
            add_issue("AE005", idx, row, "AEYN_RAW", raw)
        if canon == "NO" and not has_rule_on_root(idx, "AEYN_RAW", ["AE005"]):
            populated = any(present(row.get(c)) for c in ae_detail_cols)
            if populated:
                add_issue("AE006", idx, row, "AEYN_RAW", raw, "AEYN_RAW indicates no AE but AE detail fields are populated.")

    for idx, row in df[df["AE_PRESENT_ROW"]].iterrows():
        for col in cfg["required_when_ae_present"]:
            if row.get(col) is None:
                rule = "AE007" if col == "AE_TERM" else "AE002"
                add_issue(rule, idx, row, col, None, f"{col} is required when AE is present.")

    whitespace_cols = ["AE_TERM","AE_REL_STUDY_DRUG_RAW","AE_REL_STUDY_DRUG2_RAW","AE_COMMENT","AE_ACTION_OTHER_TXT"]
    for col in whitespace_cols:
        for idx, row in df.iterrows():
            rawv = row.get(f"RAW__{col}")
            if has_whitespace_issue(rawv) and not has_rule_on_root(idx, col, ["AE001", "AE002", "AE007", "AE031", "AE033", "AE038"]):
                add_issue("AE008", idx, row, col, rawv, f"{col} has leading/trailing whitespace.")

    for idx, row in df.iterrows():
        val = row.get("AE_SEVERITY_RAW")
        if val is not None and val not in set(cfg["allowed"]["severity"]):
            add_issue("AE009", idx, row, "AE_SEVERITY_RAW", clean(row.get("RAW__AE_SEVERITY_RAW")))

    allowed_tox = set(cfg["allowed"]["toxgr"])
    for idx, row in df.iterrows():
        rawv = clean(row.get("RAW__AE_TOXGR_RAW"))
        val = row.get("AE_TOXGR_RAW")
        if val is None:
            continue
        if val in allowed_tox:
            continue
        m = re.fullmatch(r"(?i)GRADE\s*([1-5])", str(rawv).strip()) if rawv is not None else None
        if m:
            add_issue("AE010", idx, row, "AE_TOXGR_RAW", rawv, f"Could standardize {rawv} to {m.group(1)}.")
        else:
            add_issue("AE011", idx, row, "AE_TOXGR_RAW", rawv)

    sevmap = cfg["severity_toxgr_map"]
    for idx, row in df.iterrows():
        sev = row.get("AE_SEVERITY_RAW")
        tox = row.get("AE_TOXGR_RAW")
        rawtox = clean(row.get("RAW__AE_TOXGR_RAW"))
        if has_rule_on_root(idx, "AE_SEVERITY_RAW", ["AE009"]) or has_rule_on_root(idx, "AE_TOXGR_RAW", ["AE011"]):
            continue
        norm_tox = tox
        if norm_tox not in allowed_tox and rawtox:
            m = re.fullmatch(r"(?i)GRADE\s*([1-5])", rawtox)
            if m:
                norm_tox = m.group(1)
        if sev in sevmap and norm_tox in allowed_tox and norm_tox not in set(sevmap[sev]):
            add_issue("AE012", idx, row, "AE_SEVERITY_RAW/AE_TOXGR_RAW", f"{sev} / {rawtox or tox}")

    serious_detail_cols = ["CANON__AE_SER_DTH_RAW","CANON__AE_SER_LIFE_RAW","CANON__AE_SER_HOSP_RAW","CANON__AE_SER_DISAB_RAW","CANON__AE_SER_CONG_RAW","CANON__AE_SER_MIE_RAW"]
    for idx, row in df.iterrows():
        ser = row.get("CANON__AE_SER_RAW")
        if row.get("AE_SER_RAW") is None or ser not in allowed_yn:
            add_issue("AE015", idx, row, "AE_SER_RAW", clean(row.get("RAW__AE_SER_RAW")))
            continue
        any_crit = any(row.get(c) == "YES" for c in serious_detail_cols)
        if ser != "YES" and any_crit:
            add_issue("AE013", idx, row, "AE_SER_RAW", clean(row.get("RAW__AE_SER_RAW")))
        elif ser == "YES" and not any_crit:
            add_issue("AE014", idx, row, "AE_SER_RAW", clean(row.get("RAW__AE_SER_RAW")))

    for idx, row in df.iterrows():
        for col in DATE_COLUMNS:
            status = row.get(f"{col}_STATUS")
            rawv = clean(row.get(f"RAW__{col}"))
            if status == "alt_valid":
                add_issue("AE016", idx, row, col, rawv, f"{col} is valid but non-ISO; could standardize to {row.get(f'{col}_NORM')}.")
            elif status == "invalid" and rawv is not None:
                add_issue("AE017", idx, row, col, rawv)
            elif status == "partial":
                add_issue("AE020", idx, row, col, rawv)
        for col in TIME_COLUMNS:
            status = row.get(f"{col}_STATUS")
            rawv = clean(row.get(f"RAW__{col}"))
            if status == "alt_valid":
                add_issue("AE018", idx, row, col, rawv, f"{col} is valid but non-standard; could standardize to {row.get(f'{col}_NORM')}.")
            elif status == "invalid" and rawv is not None:
                add_issue("AE019", idx, row, col, rawv)

    for idx, row in df.iterrows():
        sd = as_text(row.get("AE_START_DATE_RAW_NORM"))
        ed = as_text(row.get("AE_END_DATE_RAW_NORM"))
        st = as_text(row.get("AE_START_TIME_RAW_NORM"))
        et = as_text(row.get("AE_END_TIME_RAW_NORM"))
        rd = as_text(row.get("AE_REPORT_DATE_RAW_NORM"))
        vd = as_text(row.get("VISITDT_RAW_NORM"))
        if not has_rule_on_root(idx, "AE_START_DATE_RAW", ["AE017", "AE020"]) and not has_rule_on_root(idx, "AE_END_DATE_RAW", ["AE017", "AE020"]):
            if len(sd) == 10 and len(ed) == 10 and sd > ed:
                add_issue("AE021", idx, row, "AE_START_DATE_RAW/AE_END_DATE_RAW", f"{clean(row.get('RAW__AE_START_DATE_RAW'))} / {clean(row.get('RAW__AE_END_DATE_RAW'))}")
            if len(sd) == 10 and len(ed) == 10 and sd == ed and st and et and not has_rule_on_root(idx, "AE_START_TIME_RAW", ["AE019"]) and not has_rule_on_root(idx, "AE_END_TIME_RAW", ["AE019"]) and st > et:
                add_issue("AE022", idx, row, "AE_START_TIME_RAW/AE_END_TIME_RAW", f"{clean(row.get('RAW__AE_START_TIME_RAW'))} / {clean(row.get('RAW__AE_END_TIME_RAW'))}")
        if not has_rule_on_root(idx, "AE_REPORT_DATE_RAW", ["AE017", "AE020"]) and not has_rule_on_root(idx, "AE_START_DATE_RAW", ["AE017", "AE020"]):
            if len(sd) == 10 and len(rd) == 10 and rd < sd:
                add_issue("AE023", idx, row, "AE_REPORT_DATE_RAW", clean(row.get("RAW__AE_REPORT_DATE_RAW")))
        # AE024 intentionally relaxed: do not flag AE start after visit date in raw Layer 1 due to common visit/onset timing ambiguity

    for idx, row in df.iterrows():
        og = row.get("CANON__AE_ONGOING_RAW")
        rawog = clean(row.get("RAW__AE_ONGOING_RAW"))
        if row.get("AE_PRESENT_ROW") and (row.get("AE_ONGOING_RAW") is None or og not in allowed_yn):
            add_issue("AE025", idx, row, "AE_ONGOING_RAW", rawog)
            continue
        if og == "YES" and present(row.get("AE_END_DATE_RAW")):
            add_issue("AE026", idx, row, "AE_END_DATE_RAW", clean(row.get("RAW__AE_END_DATE_RAW")))
        elif og == "NO" and not present(row.get("AE_END_DATE_RAW")):
            add_issue("AE027", idx, row, "AE_END_DATE_RAW", None)
        if not present(row.get("AE_END_DATE_RAW")) and present(row.get("AE_END_TIME_RAW")) and not has_rule_on_root(idx, "AE_END_DATE_RAW", ["AE027"]):
            add_issue("AE028", idx, row, "AE_END_TIME_RAW", clean(row.get("RAW__AE_END_TIME_RAW")))
        outcome = row.get("CANON__AE_OUTCOME_RAW")
        if og == "YES" and outcome in {"RECOVERED/RESOLVED", "FATAL"}:
            add_issue("AE029", idx, row, "AE_OUTCOME_RAW", clean(row.get("RAW__AE_OUTCOME_RAW")))
        elif og == "NO" and outcome == "NOT RECOVERED/NOT RESOLVED":
            add_issue("AE029", idx, row, "AE_OUTCOME_RAW", clean(row.get("RAW__AE_OUTCOME_RAW")))
        if outcome == "FATAL" and row.get("CANON__AE_SER_DTH_RAW") != "YES" and not has_rule_on_root(idx, "AE_SER_RAW", ["AE015"]):
            add_issue("AE030", idx, row, "AE_OUTCOME_RAW", clean(row.get("RAW__AE_OUTCOME_RAW")))

    allowed_rel = set(cfg["allowed"]["relationship"])
    for idx, row in df.iterrows():
        rawv = clean(row.get("RAW__AE_REL_STUDY_DRUG_RAW"))
        val = as_text(row.get("AE_REL_STUDY_DRUG_RAW"))
        if row.get("AE_PRESENT_ROW") and val == "":
            add_issue("AE031", idx, row, "AE_REL_STUDY_DRUG_RAW", rawv)
            continue
        if val == "":
            continue
        if val in allowed_rel:
            continue
        if val.upper() in allowed_rel:
            add_issue("AE032", idx, row, "AE_REL_STUDY_DRUG_RAW", rawv, f"Could standardize {rawv} to {val.upper()}.")
        else:
            add_issue("AE033", idx, row, "AE_REL_STUDY_DRUG_RAW", rawv)

    # Canonical/raw-aware terminology checks:
    # Use canonical values for yes/no, outcome, reporter, and entry status fields so common raw forms are not over-flagged.
    ct_case_checks = {
        "AE_SER_RAW": ("CANON__AE_SER_RAW", cfg["allowed"]["yes_no"]),
        "AE_SER_DTH_RAW": ("CANON__AE_SER_DTH_RAW", cfg["allowed"]["yes_no"]),
        "AE_SER_LIFE_RAW": ("CANON__AE_SER_LIFE_RAW", cfg["allowed"]["yes_no"]),
        "AE_SER_HOSP_RAW": ("CANON__AE_SER_HOSP_RAW", cfg["allowed"]["yes_no"]),
        "AE_SER_DISAB_RAW": ("CANON__AE_SER_DISAB_RAW", cfg["allowed"]["yes_no"]),
        "AE_SER_CONG_RAW": ("CANON__AE_SER_CONG_RAW", cfg["allowed"]["yes_no"]),
        "AE_SER_MIE_RAW": ("CANON__AE_SER_MIE_RAW", cfg["allowed"]["yes_no"]),
        "AE_PRESPEC_RAW": ("CANON__AE_PRESPEC_RAW", cfg["allowed"]["prespecified"]),
        "AE_REPORTED_BY": ("CANON__AE_REPORTED_BY", cfg["allowed"]["reporter"]),
        "ENTRY_STATUS_RAW": ("CANON__ENTRY_STATUS_RAW", cfg["allowed"]["entry_status"]),
        "AE_OUTCOME_RAW": ("CANON__AE_OUTCOME_RAW", cfg["allowed"]["outcome"]),
    }
    for col, (canon_col, allowed_vals) in ct_case_checks.items():
        allowed = set(allowed_vals)
        upper_allowed = {v.upper() for v in allowed_vals}
        for idx, row in df.iterrows():
            rawv = clean(row.get(f"RAW__{col}"))
            raw_upper = rawv.upper() if isinstance(rawv, str) else rawv
            val = row.get(canon_col)
            if rawv is None or has_rule_on_root(idx, col, ["AE001", "AE002", "AE007"]):
                continue
            if val in allowed:
                continue
            # Only emit AE037 when the raw form differs by case only and is otherwise approved terminology.
            if raw_upper in upper_allowed and rawv not in allowed:
                add_issue("AE037", idx, row, col, rawv, f"{col} differs only by case from approved terminology.")
            else:
                if col == "AE_SER_RAW" and has_rule_on_root(idx, col, ["AE015"]):
                    continue
                add_issue("AE038", idx, row, col, rawv)

    dup_keys = ["SUBJECT_RAW","AE_TERM","AE_START_DATE_RAW_NORM"]
    dup_mask = df.duplicated(subset=dup_keys, keep=False) & df["SUBJECT_RAW"].notna() & df["AE_TERM"].notna() & df["AE_START_DATE_RAW_NORM"].notna()
    dup_groups = df.loc[dup_mask].groupby(dup_keys, dropna=False).groups
    for keyvals, indexes in dup_groups.items():
        for idx in indexes:
            row = df.loc[idx]
            other_rows = row_numbers_as_text(indexes, idx)
            msg = "Potential duplicate with row(s): " + other_rows if other_rows else "Potential duplicate record."
            add_issue("AE034", idx, row, "SUBJECT_RAW/AE_TERM/AE_START_DATE_RAW", f"{row.get('SUBJECT_RAW')} / {row.get('AE_TERM')} / {row.get('AE_START_DATE_RAW')}", msg)

    seq_mask = df["SUBJECT_RAW"].notna() & df["AE_SEQ_CRf"].notna()
    seq_groups = df.loc[seq_mask].groupby(["SUBJECT_RAW","AE_SEQ_CRf"], dropna=False).groups
    for (subj, seq), indexes in seq_groups.items():
        if len(indexes) > 1:
            for idx in indexes:
                row = df.loc[idx]
                other_rows = row_numbers_as_text(indexes, idx)
                msg = f"AE_SEQ_CRf potentially duplicated with row(s): {other_rows}" if other_rows else "AE_SEQ_CRf potentially duplicated."
                add_issue("AE035", idx, row, "AE_SEQ_CRf", row.get("AE_SEQ_CRf"), msg)

    # Keep AE036 but make it operationally lighter: only flag when AE is present and visit is not screening
    for idx, row in df[df["RAND_NO"].isna()].iterrows():
        if row.get("AE_PRESENT_ROW") and row.get("VISIT_RAW") != "SCREENING":
            add_issue("AE036", idx, row, "RAND_NO", row.get("RAND_NO"))

    for idx, row in df.iterrows():
        if row.get("ENTRY_STATUS_RAW") == "UPDATED" or present(row.get("CHANGE_REASON_RAW")):
            add_issue("AE039", idx, row, "ENTRY_STATUS_RAW/CHANGE_REASON_RAW", f"{row.get('ENTRY_STATUS_RAW')} / {row.get('CHANGE_REASON_RAW')}")

    # second-pass suppression at root level
    suppressed = set()
    for root, hits in row_rule_hits.items():
        hit_ids = {h['rule_id'] for h in hits}
        for hid in list(hit_ids):
            for child in suppress_by_rule.get(hid, []):
                if child in hit_ids:
                    for i, rec in enumerate(issues):
                        if rec['source_row_number'] == hits[0]['source_row_number'] and normalize_focus_column(rec['variable_name']) == root[1] and rec['rule_id'] == child:
                            suppressed.add(i)
        if len(hits) > 1:
            best = best_hit_for_root(root[0], root[1])
            best_pri = priority_map.get(best['rule_id'], 999) if best else 999
            for i, rec in enumerate(issues):
                if rec['source_row_number'] == hits[0]['source_row_number'] and normalize_focus_column(rec['variable_name']) == root[1]:
                    pri = priority_map.get(rec['rule_id'], 999)
                    if pri > best_pri and rec['rule_id'] in {'AE008','AE016','AE018','AE032','AE037'}:
                        suppressed.add(i)

    issues_final = [rec for i, rec in enumerate(issues) if i not in suppressed]
    for rec in issues_final:
        key = (rec['rule_id'], rec['severity'], rec['final_bucket'], rec['rule_description'])
        summary[key] = summary.get(key, 0) + 1

    issue_df = pd.DataFrame(issues_final)
    if issue_df.empty:
        issue_df = pd.DataFrame(columns=[
            "source_row_number","rule_id","severity","final_bucket","rule_description","classification_basis",
            "subject_key","visit_raw","visitnum_raw","test_code","test_name","variable_name","variable_value","message"
        ])
    issue_df = issue_df.sort_values(["source_row_number","rule_id","variable_name"], na_position="last").reset_index(drop=True)

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

    first_cols = [
        "source_row_number","rule_id","severity","final_bucket","rule_description","classification_basis",
        "subject_key","visit_raw","visitnum_raw","test_code","test_name","variable_name","variable_value","message"
    ]
    issue_df = issue_df[first_cols]

    snapshot_df = raw_df.copy()
    if "L1_SOURCE_ROW_NUMBER" not in snapshot_df.columns:
        snapshot_df.insert(0, "L1_SOURCE_ROW_NUMBER", range(1, len(snapshot_df) + 1))

    issue_df.to_csv(outdir / "ae_issue_log_all.csv", index=False)
    issue_df[issue_df["final_bucket"] == "Human"].to_csv(outdir / "ae_issue_log_human.csv", index=False)
    issue_df[issue_df["final_bucket"] == "SDTM_STANDARDISABLE"].to_csv(outdir / "ae_issue_log_sdtm_standardisable.csv", index=False)
    summary_df.to_csv(outdir / "ae_issue_summary_by_rule.csv", index=False)
    snapshot_df.to_csv(outdir / "ae_source_snapshot.csv", index=False)

    metadata = {
        "script_name": Path(__file__).name,
        "rules_json": str(rules_path.name),
        "input_csv": str(source.name),
        "output_dir": str(outdir),
        "issue_count_total": int(len(issue_df)),
        "issue_count_human": int((issue_df["final_bucket"] == "Human").sum()) if not issue_df.empty else 0,
        "issue_count_sdtm_standardisable": int((issue_df["final_bucket"] == "SDTM_STANDARDISABLE").sum()) if not issue_df.empty else 0
    }
    (outdir / "ae_run_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(f"Created outputs in: {outdir}")
    print(f"Input file used: {source}")


if __name__ == "__main__":
    main()
