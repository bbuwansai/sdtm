from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

NULLS = {"", "NA", "NAN", "NONE", "NULL"}
RAW_REQUIRED_COLUMNS = [
    "ROW_ID","STUDYID_RAW","SITEID_RAW","SUBJECT_RAW","SCREENING_NO","RAND_NO","VISIT_RAW","VISITDT_RAW",
    "AE_FORM_SEQ","AE_SEQ_CRf","AEYN_RAW","AE_TERM","AE_START_DATE_RAW","AE_START_TIME_RAW","AE_END_DATE_RAW",
    "AE_END_TIME_RAW","AE_ONGOING_RAW","AE_SEVERITY_RAW","AE_TOXGR_RAW","AE_SER_RAW","AE_SER_DTH_RAW",
    "AE_SER_LIFE_RAW","AE_SER_HOSP_RAW","AE_SER_DISAB_RAW","AE_SER_CONG_RAW","AE_SER_MIE_RAW",
    "AE_REL_STUDY_DRUG_RAW","AE_REL_STUDY_DRUG2_RAW","AE_ACTION_DRUG_RAW","AE_ACTION_DRUG2_RAW",
    "AE_ACTION_OTHER_TXT","AE_OUTCOME_RAW","AE_PRESPEC_RAW","AE_REPORTED_BY","AE_REPORT_DATE_RAW",
    "AE_COMMENT","ENTRY_STATUS_RAW","CHANGE_REASON_RAW"
]

SPEC_FILES = {
    "spec": ["ae_mapping_spec_validated_v2.csv", "ae_mapping_spec_validated_v1.csv"],
    "visit_map": ["ae_visit_map_v2.csv", "ae_visit_map_v1.csv"],
    "rel_action_map": ["ae_relationship_action_map_v2.csv", "ae_relationship_action_map_v1.csv"],
}

LAYER1_FILES = {
    "human_issues": ["ae_issue_log_human.csv"],
    "sdtm_issues": ["ae_issue_log_sdtm_standardisable.csv"],
}

RAW_CANDIDATES = ["ae_raw_synthetic.csv", "ae_raw.csv", "ae_source.csv"]
MEDDRA_MAP_CANDIDATES = ["ae_meddra_map.csv", "ae_meddra_coding_map.csv", "ae_meddra_demo_map_template.csv"]


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


def norm_upper_space(v) -> Optional[str]:
    v = clean(v)
    if v is None:
        return None
    return re.sub(r"\s+", " ", str(v).strip().upper())


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
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?", s):
        try:
            ts = pd.to_datetime(s, errors="raise")
            return ts.strftime("%Y-%m-%d"), "alt_valid"
        except Exception:
            return None, "invalid"
    for fmt in ("%Y/%m/%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%d/%m/%Y", "%d%b%Y"):
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


def canonical_num_text(v) -> Optional[str]:
    n = parse_num(v)
    if n is None:
        return None
    return str(int(n)) if float(n).is_integer() else str(n)


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
    return score


def find_best_file(project_folder: Path, candidates: List[str]) -> Optional[Path]:
    all_files = [p for p in project_folder.rglob("*") if p.is_file()]
    matched = [p for p in all_files if any(c.lower() in p.name.lower() for c in candidates)]
    if not matched:
        return None
    return sorted(matched, key=lambda p: (file_score(p, candidates), str(p)), reverse=True)[0]


def auto_detect_inputs(project_folder: Path) -> Dict[str, Optional[Path]]:
    resolved: Dict[str, Optional[Path]] = {}
    for key, names in SPEC_FILES.items():
        p = find_best_file(project_folder, names)
        resolved[key] = p
    for key, names in LAYER1_FILES.items():
        p = find_best_file(project_folder, names)
        if p is None:
            raise FileNotFoundError(f"Could not auto-detect required Layer 1 file for {key}: {names}")
        resolved[key] = p
    raw = find_best_file(project_folder, RAW_CANDIDATES)
    if raw is None:
        raise FileNotFoundError("Could not auto-detect AE raw source file")
    resolved["raw"] = raw
    resolved["meddra_map"] = find_best_file(project_folder, MEDDRA_MAP_CANDIDATES)
    return resolved


def load_csv(path: Path, label: str) -> pd.DataFrame:
    if path is None or not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return pd.read_csv(path, dtype=str)


def ensure_columns(df: pd.DataFrame, cols: List[str], label: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def load_visit_map(path: Optional[Path]) -> Dict[str, Dict[str, Optional[str]]]:
    if path is None or not path.exists():
        # fallback demo visit map
        data = pd.DataFrame([
            ["SCREENING", 10], ["BASELINE", 20], ["DAY 1", 20], ["WEEK 1", 30], ["WEEK 2", 40],
            ["WEEK 4", 50], ["WEEK 8", 60], ["UNSCHEDULED", 99],
        ], columns=["visit_name", "visitnum"])
    else:
        data = pd.read_csv(path, dtype=str)
    out = {}
    for _, r in data.iterrows():
        key = norm_upper_space(r.get("visit_name"))
        if key:
            out[key] = {"VISITNUM": canonical_num_text(r.get("visitnum"))}
    return out


def load_rel_action_map(path: Optional[Path]) -> Dict[Tuple[str, str], str]:
    out: Dict[Tuple[str, str], str] = {}
    if path is None or not path.exists():
        return out
    data = pd.read_csv(path, dtype=str)
    for _, r in data.iterrows():
        src = r.get("source_variable")
        raw = norm_upper_space(r.get("raw_value"))
        tgt = clean(r.get("target_value"))
        if src and raw and tgt:
            out[(src, raw)] = tgt
    return out


def build_demo_meddra_map() -> pd.DataFrame:
    return pd.DataFrame([
        ["HEADACHE", "Headache", "Nervous system disorders", "Nervous system disorders"],
        ["VOMITING", "Vomiting", "Gastrointestinal disorders", "Gastrointestinal disorders"],
        ["NAUSEA", "Nausea", "Gastrointestinal disorders", "Gastrointestinal disorders"],
        ["PYREXIA", "Pyrexia", "General disorders and administration site conditions", "General disorders and administration site conditions"],
        ["ALT INCREASED", "Alanine aminotransferase increased", "Investigations", "Investigations"],
        ["RASH", "Rash", "Skin and subcutaneous tissue disorders", "Skin and subcutaneous tissue disorders"],
        ["DIZZINESS", "Dizziness", "Nervous system disorders", "Nervous system disorders"],
        ["DEHYDRATION", "Dehydration", "Metabolism and nutrition disorders", "Metabolism and nutrition disorders"],
        ["INJECTION SITE PAIN", "Injection site pain", "General disorders and administration site conditions", "General disorders and administration site conditions"],
        ["SINUS TACHYCARDIA", "Sinus tachycardia", "Cardiac disorders", "Cardiac disorders"],
        ["FATIGUE", "Fatigue", "General disorders and administration site conditions", "General disorders and administration site conditions"],
        ["ANAPHYLACTIC REACTION", "Anaphylactic reaction", "Immune system disorders", "Immune system disorders"],
    ], columns=["AETERM_RAW", "AEDECOD", "AESOC", "AEBODSYS"])


def load_meddra_map(path: Optional[Path]) -> Tuple[pd.DataFrame, str]:
    if path is not None and path.exists():
        df = pd.read_csv(path, dtype=str)
        cols = {c.upper(): c for c in df.columns}
        required = ["AETERM_RAW", "AEDECOD", "AESOC", "AEBODSYS"]
        if all(col in cols for col in required):
            sub = df[[cols[c] for c in required]].copy()
            sub.columns = required
            return sub, "external"
    return build_demo_meddra_map(), "demo"


def map_yn(v: Optional[str]) -> Optional[str]:
    s = norm_upper_space(v)
    if s in {"Y", "YES"}:
        return "Y"
    if s in {"N", "NO"}:
        return "N"
    return None


def map_severity(v: Optional[str]) -> Optional[str]:
    s = norm_upper_space(v)
    m = {
        "MILD": "MILD",
        "MODERATE": "MODERATE",
        "SEVERE": "SEVERE",
    }
    return m.get(s)


def map_tox(v: Optional[str]) -> Optional[str]:
    s = norm_upper_space(v)
    if s is None:
        return None
    s = s.replace("GRADE ", "")
    return s if s in {"1", "2", "3", "4", "5"} else None


def map_outcome(v: Optional[str], rel_map: Dict[Tuple[str, str], str]) -> Optional[str]:
    s = norm_upper_space(v)
    if s is None:
        return None
    return rel_map.get(("AE_OUTCOME_RAW", s), clean(s))


def map_rel(v: Optional[str], rel_map: Dict[Tuple[str, str], str]) -> Optional[str]:
    s = norm_upper_space(v)
    if s is None:
        return None
    return rel_map.get(("AE_REL_STUDY_DRUG_RAW", s), clean(s))


def map_action(v: Optional[str], rel_map: Dict[Tuple[str, str], str]) -> Optional[str]:
    s = norm_upper_space(v)
    if s is None:
        return None
    return rel_map.get(("AE_ACTION_DRUG_RAW", s), clean(s))


def derive_iso_dtc(date_raw: Optional[str], time_raw: Optional[str]) -> Tuple[Optional[str], List[str], List[str]]:
    reasons: List[str] = []
    autos: List[str] = []
    d_norm, d_status = parse_date_isoish(date_raw)
    t_norm, t_status = parse_time(time_raw)
    if d_norm is None:
        reasons.append("Invalid or missing date")
        return None, reasons, autos
    if t_norm is not None:
        if t_status == "alt_valid":
            autos.append("Standardized non-standard time to HH:MM")
        return f"{d_norm}T{t_norm}", reasons, autos
    return d_norm, reasons, autos


def build_subject_id(studyid: Optional[str], siteid: Optional[str], subject: Optional[str]) -> Optional[str]:
    if all(present(x) for x in [studyid, siteid, subject]):
        return f"{studyid}-{siteid}-{subject}"
    return None


def extract_duplicate_candidates(human_log: pd.DataFrame) -> pd.DataFrame:
    if human_log.empty:
        return pd.DataFrame(columns=["source_row_number", "rule_id", "message"])
    tmp = human_log.copy()
    tmp["source_row_number"] = pd.to_numeric(tmp["source_row_number"], errors="coerce")
    return tmp[tmp["rule_id"].isin(["AE034", "AE035"])].sort_values(["source_row_number", "rule_id"])


def make_final_duplicate_key(df: pd.DataFrame) -> pd.Series:
    key_cols = ["STUDYID", "USUBJID", "AETERM", "AESTDTC", "AEDECOD", "AESER", "AESEV", "AEOUT"]
    return df[key_cols].fillna("").agg("|".join, axis=1)


def generate_ae(project_folder: Path, outdir: Path, meddra_version: str = "27.1", keep_uncoded_rows: str = "N"):
    resolved = auto_detect_inputs(project_folder)

    raw = load_csv(resolved["raw"], "Raw AE")
    spec = load_csv(resolved["spec"], "AE spec")
    human_log = load_csv(resolved["human_issues"], "Human issue log")
    sdtm_log = load_csv(resolved["sdtm_issues"], "SDTM-standardisable issue log")

    ensure_columns(raw, RAW_REQUIRED_COLUMNS, "Raw AE")
    ensure_columns(spec, ["target_variable"], "AE spec")
    ensure_columns(human_log, ["source_row_number", "rule_id", "message"], "Human issue log")
    ensure_columns(sdtm_log, ["source_row_number", "rule_id", "message"], "SDTM-standardisable issue log")

    if "L1_SOURCE_ROW_NUMBER" not in raw.columns:
        raw.insert(0, "L1_SOURCE_ROW_NUMBER", range(1, len(raw) + 1))

    for c in raw.columns:
        raw[c] = raw[c].apply(clean)
    raw["L1_SOURCE_ROW_NUMBER"] = pd.to_numeric(raw["L1_SOURCE_ROW_NUMBER"], errors="coerce").astype(int)

    visit_lookup = load_visit_map(resolved.get("visit_map"))
    rel_action_map = load_rel_action_map(resolved.get("rel_action_map"))
    meddra_map_df, coding_source = load_meddra_map(resolved.get("meddra_map"))
    meddra_lookup = {}
    for _, r in meddra_map_df.iterrows():
        key = norm_upper_space(r["AETERM_RAW"])
        if key:
            meddra_lookup[key] = {
                "AEDECOD": clean(r["AEDECOD"]),
                "AESOC": clean(r["AESOC"]),
                "AEBODSYS": clean(r["AEBODSYS"]),
            }

    human_agg = summarize_issue_log(human_log)
    sdtm_agg = summarize_issue_log(sdtm_log)
    human_rows = set(human_agg["source_row_number"].tolist()) if not human_agg.empty else set()
    sdtm_rows = set(sdtm_agg["source_row_number"].tolist()) if not sdtm_agg.empty else set()

    human_rejected = raw[raw["L1_SOURCE_ROW_NUMBER"].isin(human_rows)].copy()
    if not human_rejected.empty:
        human_rejected = human_rejected.merge(human_agg, how="left", left_on="L1_SOURCE_ROW_NUMBER", right_on="source_row_number")
        human_rejected["exception_type"] = "LAYER1_HUMAN"

    candidate_raw = raw[~raw["L1_SOURCE_ROW_NUMBER"].isin(human_rows)].copy()

    spec_target_order = spec["target_variable"].dropna().astype(str).tolist()
    final_rows: List[dict] = []
    transform_exceptions: List[dict] = []
    auto_logs: List[dict] = []

    for _, row in candidate_raw.iterrows():
        src_row = int(row["L1_SOURCE_ROW_NUMBER"])
        reasons: List[str] = []
        autos: List[str] = []

        studyid = norm_text(row.get("STUDYID_RAW"))
        usubjid = build_subject_id(studyid, norm_text(row.get("SITEID_RAW")), norm_text(row.get("SUBJECT_RAW")))
        if not present(studyid):
            reasons.append("Missing STUDYID")
        if not present(usubjid):
            reasons.append("Cannot derive USUBJID")

        ae_present = map_yn(row.get("AEYN_RAW"))
        if ae_present == "N":
            reasons.append("AEYN indicates no AE record")
        aeterm = norm_text(row.get("AE_TERM"))
        if not present(aeterm):
            reasons.append("Missing AETERM")

        aestdtc, r1, a1 = derive_iso_dtc(row.get("AE_START_DATE_RAW"), row.get("AE_START_TIME_RAW"))
        reasons += r1
        autos += a1
        aeendtc = None
        if present(row.get("AE_END_DATE_RAW")):
            aeendtc, r2, a2 = derive_iso_dtc(row.get("AE_END_DATE_RAW"), row.get("AE_END_TIME_RAW"))
            reasons += r2
            autos += a2
        elif present(row.get("AE_END_TIME_RAW")):
            reasons.append("End time present without end date")

        aeenrf = None
        ongoing = map_yn(row.get("AE_ONGOING_RAW"))
        if ongoing == "Y" and not present(aeendtc):
            aeenrf = "ONGOING"
        elif ongoing == "Y" and present(aeendtc):
            reasons.append("Ongoing flag conflicts with end date")

        meddra = meddra_lookup.get(norm_upper_space(aeterm) if aeterm else None)
        if meddra is None:
            if keep_uncoded_rows == "Y":
                autos.append("Kept row without MedDRA coding by policy")
                aedecod = None
                aesoc = None
                aebodsys = None
                aedictv = None
            else:
                reasons.append("No MedDRA coding available for AETERM")
                aedecod = aesoc = aebodsys = aedictv = None
        else:
            aedecod = meddra["AEDECOD"]
            aesoc = meddra["AESOC"]
            aebodsys = meddra["AEBODSYS"]
            aedictv = f"MedDRA {meddra_version}"

        aesev = map_severity(row.get("AE_SEVERITY_RAW"))
        aetoxgr = map_tox(row.get("AE_TOXGR_RAW"))
        aeser = map_yn(row.get("AE_SER_RAW"))
        aesdth = map_yn(row.get("AE_SER_DTH_RAW"))
        aeslife = map_yn(row.get("AE_SER_LIFE_RAW"))
        aeshosp = map_yn(row.get("AE_SER_HOSP_RAW"))
        aesdisab = map_yn(row.get("AE_SER_DISAB_RAW"))
        aescong = map_yn(row.get("AE_SER_CONG_RAW"))
        aesmie = map_yn(row.get("AE_SER_MIE_RAW"))
        aerel = map_rel(row.get("AE_REL_STUDY_DRUG_RAW"), rel_action_map)
        aeacn = map_action(row.get("AE_ACTION_DRUG_RAW"), rel_action_map)
        aeacnoth = norm_text(row.get("AE_ACTION_OTHER_TXT"))
        aeout = map_outcome(row.get("AE_OUTCOME_RAW"), rel_action_map)
        aepresp = map_yn(row.get("AE_PRESPEC_RAW"))
        visit = norm_upper_space(row.get("VISIT_RAW"))
        visitnum = None
        if visit and visit in visit_lookup:
            visitnum = parse_num(visit_lookup[visit]["VISITNUM"])
        elif visit:
            reasons.append("VISIT not in approved visit map")
        aespid = norm_text(row.get("ROW_ID"))
        aegrpid = canonical_num_text(row.get("AE_FORM_SEQ"))
        aerefid = canonical_num_text(row.get("AE_SEQ_CRf"))

        # additional strict CDISC-like checks for survived rows
        if not present(aestdtc):
            reasons.append("Missing required AESTDTC")
        if present(aeendtc) and present(aestdtc):
            if len(aestdtc) >= 10 and len(aeendtc) >= 10:
                try:
                    if pd.to_datetime(aestdtc) > pd.to_datetime(aeendtc):
                        reasons.append("AESTDTC occurs after AEENDTC")
                except Exception:
                    pass
        if aeout == "FATAL" and aesdth != "Y":
            reasons.append("FATAL outcome without AESDTH=Y")
        if aeser == "N" and any(x == "Y" for x in [aesdth, aeslife, aeshosp, aesdisab, aescong, aesmie]):
            reasons.append("Seriousness criterion populated while AESER=N")
        if ongoing == "N" and not present(aeendtc):
            reasons.append("Non-ongoing event missing AEENDTC")
        if aesev is None and present(row.get("AE_SEVERITY_RAW")):
            reasons.append("Invalid AESEV source value")
        if aetoxgr is None and present(row.get("AE_TOXGR_RAW")):
            reasons.append("Invalid AETOXGR source value")
        if aeser is None and present(row.get("AE_SER_RAW")):
            reasons.append("Invalid AESER source value")

        if reasons:
            transform_exceptions.append({
                "source_row_number": src_row,
                "exception_type": "TRANSFORM_UNRESOLVED",
                "reason": " | ".join(sorted(set(reasons))),
                **row.to_dict(),
            })
            continue

        final_rows.append({
            "STUDYID": studyid,
            "DOMAIN": "AE",
            "USUBJID": usubjid,
            "AESEQ": None,
            "AETERM": aeterm,
            "AEMODIFY": None,
            "AEDECOD": aedecod,
            "AESOC": aesoc,
            "AEBODSYS": aebodsys,
            "AESEV": aesev,
            "AETOXGR": aetoxgr,
            "AESER": aeser,
            "AESDTH": aesdth,
            "AESLIFE": aeslife,
            "AESHOSP": aeshosp,
            "AESDISAB": aesdisab,
            "AESCONG": aescong,
            "AESMIE": aesmie,
            "AEREL": aerel,
            "AEACN": aeacn,
            "AEACNOTH": aeacnoth,
            "AEOUT": aeout,
            "AEPRESP": aepresp,
            "AESTDTC": aestdtc,
            "AEENDTC": aeendtc,
            "AEENRF": aeenrf,
            "VISIT": visit,
            "VISITNUM": visitnum,
            "EPOCH": None,
            "AESPID": aespid,
            "AEGRPID": aegrpid,
            "AEREFID": aerefid,
            "AEDICTV": aedictv,
            "_SOURCE_ROW_NUMBER": src_row,
            "_FINAL_DUP_KEY": None,
        })

        if autos or src_row in sdtm_rows:
            auto_logs.append({
                "source_row_number": src_row,
                "auto_actions": " | ".join(autos) if autos else "Row had Layer 1 SDTM-standardisable issue(s); deterministic transform completed."
            })

    final_df = pd.DataFrame(final_rows)
    exc_df = pd.DataFrame(transform_exceptions)
    auto_df = pd.DataFrame(auto_logs)
    exact_dups_dropped = pd.DataFrame()

    if not final_df.empty:
        final_df = final_df.sort_values(["STUDYID", "USUBJID", "AESTDTC", "AEDECOD", "VISITNUM", "_SOURCE_ROW_NUMBER"], na_position="last").reset_index(drop=True)
        final_df["_FINAL_DUP_KEY"] = make_final_duplicate_key(final_df)
        dup_mask = final_df.duplicated(subset=["_FINAL_DUP_KEY"], keep="first")
        if dup_mask.any():
            exact_dups_dropped = final_df[dup_mask].copy()
            exact_dups_dropped["drop_reason"] = "Exact duplicate after final SDTM AE mapping"
            final_df = final_df[~dup_mask].copy()
        final_df["AESEQ"] = final_df.groupby("USUBJID", dropna=False).cumcount() + 1
        final_df = final_df.drop(columns=["_FINAL_DUP_KEY"])

    output_cols = [c for c in spec_target_order if c in final_df.columns]
    final_ordered = final_df[output_cols].copy() if not final_df.empty else pd.DataFrame(columns=output_cols)

    duplicate_candidates = extract_duplicate_candidates(human_log)
    build_summary = pd.DataFrame([
        {"metric": "raw_input_rows", "value": len(raw)},
        {"metric": "rows_rejected_by_layer1_human", "value": 0 if human_rejected.empty else len(human_rejected)},
        {"metric": "rows_after_human_gate", "value": len(candidate_raw)},
        {"metric": "rows_routed_to_transform_exceptions", "value": 0 if exc_df.empty else len(exc_df)},
        {"metric": "duplicate_candidates_flagged_in_layer1", "value": 0 if duplicate_candidates.empty else len(duplicate_candidates)},
        {"metric": "rows_dropped_as_exact_duplicates", "value": 0 if exact_dups_dropped.empty else len(exact_dups_dropped)},
        {"metric": "final_ae_rows", "value": len(final_ordered)},
        {"metric": "rows_auto_standardized_logged", "value": 0 if auto_df.empty else len(auto_df)},
        {"metric": "meddra_coding_source", "value": coding_source},
        {"metric": "meddra_dictionary_version_used", "value": f"MedDRA {meddra_version}"},
    ])

    final_df.to_csv(outdir / "ae_final_sdtm.csv", index=False)
    final_ordered.to_csv(outdir / "ae_final_sdtm_ordered.csv", index=False)
    human_rejected.to_csv(outdir / "ae_exceptions_human.csv", index=False)
    exc_df.to_csv(outdir / "ae_exceptions_transform.csv", index=False)
    exact_dups_dropped.to_csv(outdir / "ae_exact_duplicates_dropped.csv", index=False)
    auto_df.to_csv(outdir / "ae_auto_standardized_log.csv", index=False)
    duplicate_candidates.to_csv(outdir / "ae_duplicate_candidates_from_layer1.csv", index=False)
    build_summary.to_csv(outdir / "ae_build_summary.csv", index=False)
    pd.DataFrame([{"file_role": k, "path": str(v) if v else None} for k, v in resolved.items()]).to_csv(outdir / "ae_detected_inputs.csv", index=False)

    if coding_source == "demo":
        demo_map = build_demo_meddra_map()
        demo_map.to_csv(outdir / "ae_meddra_demo_map_template.csv", index=False)

    print(f"Created outputs in: {outdir}")
    print("Detected inputs:")
    for k, v in resolved.items():
        print(f"  {k}: {v}")
    print(f"MedDRA dictionary version used: MedDRA {meddra_version}")


def main():
    parser = argparse.ArgumentParser(description="Fast auto-detecting AE SDTM generator v2")
    parser.add_argument("--project-folder", default=".", help="Folder containing raw/spec/layer1 files")
    parser.add_argument("--outdir", default=None, help="Optional output folder. Default: <project-folder>/ae_sdtm_output_v2")
    parser.add_argument("--meddra-version", default="27.1", help="Dictionary version label to populate in AEDICTV when coding is available")
    parser.add_argument("--keep-uncoded-rows", choices=["Y", "N"], default="N")
    args = parser.parse_args()

    project_folder = Path(args.project_folder).resolve()
    outdir = Path(args.outdir).resolve() if args.outdir else (project_folder / "ae_sdtm_output_v2")
    outdir.mkdir(parents=True, exist_ok=True)
    generate_ae(project_folder=project_folder, outdir=outdir, meddra_version=args.meddra_version, keep_uncoded_rows=args.keep_uncoded_rows)


if __name__ == "__main__":
    main()
