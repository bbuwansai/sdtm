import re
from pathlib import Path
from typing import Optional, Dict, Tuple
import pandas as pd

BASE = Path(__file__).resolve().parent
SOURCE_CANDIDATES = [
    BASE / "vs_layer1_outputs_v4" / "vs_cleaned_output_v4.csv",
    BASE / "vs_layer1_outputs_v3" / "vs_cleaned_output_v3.csv",
    BASE / "vs_layer1_outputs_v2" / "vs_cleaned_output_v2.csv",
    BASE / "vs_layer1_outputs" / "vs_cleaned_output.csv",
    BASE / "vs_cleaned_output_v4.csv",
    BASE / "vs_cleaned_output_v3.csv",
    BASE / "vs_cleaned_output_v2.csv",
    BASE / "vs_cleaned_output.csv",
]
SPEC_DIR = BASE / "vs_spec_outputs_v3"
OUTDIR = BASE / "vs_sdtm_outputs_v4"

ALLOW_PARTIAL_VSDTC_IN_FINAL = False

SUBMISSION_COLS = [
    "STUDYID","DOMAIN","USUBJID","VSSEQ","VSTESTCD","VSTEST","VSDTC",
    "VISIT","VISITNUM","VSORRES","VSORRESU","VSSTRESC","VSSTRESN","VSSTRESU",
    "VSPOS","VSFAST"
]

CHAR_COLS = [
    "STUDYID","DOMAIN","USUBJID","VSTESTCD","VSTEST","VSDTC","VISIT",
    "VSORRES","VSORRESU","VSSTRESC","VSSTRESU","VSPOS","VSFAST"
]

def first_existing(candidates):
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"None of the source candidates exist: {[str(p) for p in candidates]}")

def clean(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    return None if s.upper() in {"", "NA", "NULL", "NAN", "NONE"} else s

def to_iso_partial(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if re.fullmatch(r"\d{4}", s) or re.fullmatch(r"\d{4}-\d{2}", s) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    return None

def is_partial_date(v: Optional[str]) -> bool:
    return isinstance(v, str) and len(v) in {4, 7}

def parse_num(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", s):
        return float(s)
    return None

def load_tables():
    source = first_existing(SOURCE_CANDIDATES)
    df = pd.read_csv(source, dtype=str)
    for c in df.columns:
        df[c] = df[c].apply(clean)

    spec = pd.read_csv(SPEC_DIR / "vs_mapping_spec_validated_v3.csv", dtype=str)
    test_map = pd.read_csv(SPEC_DIR / "vs_test_map_v3.csv", dtype=str)
    unit_map = pd.read_csv(SPEC_DIR / "vs_unit_map_v3.csv", dtype=str)
    conv = pd.read_csv(SPEC_DIR / "vs_conversion_rules_v3.csv", dtype=str)
    visit_map = pd.read_csv(SPEC_DIR / "vs_visit_map_v3.csv", dtype=str)
    impl = pd.read_csv(SPEC_DIR / "vs_implausibility_thresholds_v3.csv", dtype=str)
    pos = pd.read_csv(SPEC_DIR / "vs_position_map_v3.csv", dtype=str)
    fast = pd.read_csv(SPEC_DIR / "vs_fasting_map_v3.csv", dtype=str)
    return source, df, spec, test_map, unit_map, conv, visit_map, impl, pos, fast

def build_lookups(test_map, unit_map, conv, visit_map, impl, pos, fast):
    test_lut = {}
    for _, r in test_map.iterrows():
        test_lut[str(r["raw_test_name"]).strip().upper()] = {
            "VSTESTCD": str(r["vstestcd"]).strip(),
            "VSTEST": str(r["vstest"]).strip(),
        }

    unit_lut = {}
    for _, r in unit_map.iterrows():
        unit_lut[(str(r["vstestcd"]).strip(), str(r["raw_unit"]).strip().upper())] = {
            "VSORRESU": str(r["vsorresu_normalized"]).strip(),
            "VSSTRESU": str(r["vsstresu_standard"]).strip(),
            "CONVERSION_RULE": str(r["conversion_rule"]).strip(),
        }

    conv_lut = {}
    for _, r in conv.iterrows():
        conv_lut[(str(r["vstestcd"]).strip(), str(r["conversion_rule"]).strip())] = {
            "formula": str(r["formula"]).strip(),
            "rounding": str(r["rounding"]).strip(),
        }

    visit_lut = {}
    for _, r in visit_map.iterrows():
        visit_lut[str(r["visit_name"]).strip().upper()] = str(r["visitnum"]).strip()

    impl_lut = {}
    for _, r in impl.iterrows():
        impl_lut[str(r["vstestcd"]).strip()] = {
            "unit": str(r["standard_unit"]).strip(),
            "min": float(r["min_value"]),
            "max": float(r["max_value"]),
        }

    pos_lut = {str(r["raw_position"]).strip().upper(): str(r["vspos"]).strip() for _, r in pos.iterrows()}
    fast_lut = {str(r["raw_fasting"]).strip().upper(): str(r["vsfast"]).strip() for _, r in fast.iterrows()}
    return test_lut, unit_lut, conv_lut, visit_lut, impl_lut, pos_lut, fast_lut

def round_value(val: float, rounding_text: str) -> float:
    m = re.search(r"(\d+)", rounding_text or "")
    if not m:
        return val
    return round(val, int(m.group(1)))

def apply_conversion(vstestcd: str, num: float, conversion_rule: str, conv_lut: Dict[Tuple[str, str], Dict[str, str]]) -> Optional[float]:
    if conversion_rule in {"", "no conversion", None}:
        return num
    if (vstestcd, conversion_rule) not in conv_lut:
        return None
    if vstestcd == "TEMP" and conversion_rule == "F_to_C":
        out = (num - 32.0) * 5.0 / 9.0
    elif vstestcd == "TEMP" and conversion_rule == "K_to_C":
        out = num - 273.15
    elif vstestcd == "WEIGHT" and conversion_rule == "lb_to_kg":
        out = num * 0.45359237
    elif vstestcd == "HEIGHT" and conversion_rule == "in_to_cm":
        out = num * 2.54
    else:
        return None
    return round_value(out, conv_lut[(vstestcd, conversion_rule)]["rounding"])

def derive_usubjid(row: pd.Series) -> Optional[str]:
    if row.get("SUBJECT_KEY"):
        return row["SUBJECT_KEY"]
    if row.get("PROTOCOL_ID") and row.get("SITE_NUMBER") and row.get("SUBJECT_NUMBER"):
        return f"{row['PROTOCOL_ID']}-{row['SITE_NUMBER']}-{row['SUBJECT_NUMBER']}"
    return None

def issue(code, message, severity="ERROR"):
    return {"issue_code": code, "issue_message": message, "severity": severity}

def assign_vsseq(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df["VSSEQ"] = pd.Series(dtype="Int64")
        return df
    work = df.copy()

    def visitnum_sort(v):
        try:
            return float(v)
        except Exception:
            return float("inf")

    work["_VISITNUM_SORT"] = work["VISITNUM"].apply(visitnum_sort)
    work["_VSDTC_SORT"] = work["VSDTC"].fillna("9999-99-99")
    work["_VSTESTCD_SORT"] = work["VSTESTCD"].fillna("ZZZZZZ")
    work = work.sort_values(["USUBJID","_VISITNUM_SORT","_VSDTC_SORT","_VSTESTCD_SORT","SOURCE_ROW_NUMBER"]).copy()
    work["VSSEQ"] = work.groupby(["STUDYID","USUBJID"]).cumcount() + 1
    work["VSSEQ"] = work["VSSEQ"].astype("Int64")
    return work.drop(columns=["_VISITNUM_SORT","_VSDTC_SORT","_VSTESTCD_SORT"])

def enforce_output_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in CHAR_COLS:
        if col in out.columns:
            out[col] = out[col].apply(lambda x: "" if pd.isna(x) else str(x))
    if "VISITNUM" in out.columns:
        out["VISITNUM"] = pd.to_numeric(out["VISITNUM"], errors="coerce")
    if "VSSTRESN" in out.columns:
        out["VSSTRESN"] = pd.to_numeric(out["VSSTRESN"], errors="coerce")
    if "VSSEQ" in out.columns:
        out["VSSEQ"] = pd.to_numeric(out["VSSEQ"], errors="coerce").astype("Int64")
    return out

def main():
    OUTDIR.mkdir(exist_ok=True)
    source_path, df, spec, test_map, unit_map, conv, visit_map, impl, pos, fast = load_tables()
    test_lut, unit_lut, conv_lut, visit_lut, impl_lut, pos_lut, fast_lut = build_lookups(test_map, unit_map, conv, visit_map, impl, pos, fast)

    pass_rows, exc_rows, qc_rows = [], [], []
    all_source_rows = set()

    for idx, row in df.iterrows():
        src_row_num = row.get("L1_SOURCE_ROW_NUMBER") or str(idx + 1)
        all_source_rows.add(str(src_row_num))
        row_issues = []
        out = {c: None for c in SUBMISSION_COLS}

        out["STUDYID"] = row.get("PROTOCOL_ID")
        out["DOMAIN"] = "VS"
        out["USUBJID"] = derive_usubjid(row)
        out["VSDTC"] = to_iso_partial(row.get("VS_DATE"))
        out["VISIT"] = row.get("VISIT_NAME")
        if row.get("VISIT_NUM") and str(row["VISIT_NUM"]).replace(".", "", 1).isdigit():
            out["VISITNUM"] = str(int(float(row["VISIT_NUM"])))

        visit_key = str(out["VISIT"]).strip().upper() if out["VISIT"] else None
        expected_visitnum = visit_lut.get(visit_key) if visit_key else None
        if visit_key and expected_visitnum is None:
            row_issues.append(issue("VS_VISIT_001", "VISIT not found in visit map"))
        if visit_key and expected_visitnum is not None and out["VISITNUM"] is not None and out["VISITNUM"] != expected_visitnum:
            row_issues.append(issue("VS_VISIT_002", "VISIT/VISITNUM mismatch with visit map"))

        raw_test = str(row.get("VS_TEST_RAW")).strip().upper() if row.get("VS_TEST_RAW") else None
        mapped_test = test_lut.get(raw_test) if raw_test else None
        if mapped_test:
            out["VSTESTCD"] = mapped_test["VSTESTCD"]
            out["VSTEST"] = mapped_test["VSTEST"]
        else:
            row_issues.append(issue("VS_TEST_001", "VS_TEST_RAW not mapped to sponsor terminology"))

        out["VSORRES"] = None if row.get("VS_RESULT_RAW") is None else str(row.get("VS_RESULT_RAW"))
        raw_unit = row.get("VS_UNIT_RAW")
        unit_info = None
        if out["VSTESTCD"] and raw_unit:
            unit_info = unit_lut.get((out["VSTESTCD"], str(raw_unit).strip().upper()))
            if unit_info:
                out["VSORRESU"] = unit_info["VSORRESU"]
            else:
                row_issues.append(issue("VS_UNIT_001", "Unsupported raw unit for mapped test"))
        elif out["VSTESTCD"] and raw_unit is None and out["VSORRES"] is not None:
            row_issues.append(issue("VS_UNIT_002", "Missing raw unit for numeric VS test"))

        if row.get("POSITION_RAW"):
            pos_val = pos_lut.get(str(row["POSITION_RAW"]).strip().upper())
            if pos_val:
                out["VSPOS"] = pos_val
            else:
                row_issues.append(issue("VS_POS_001", "Unsupported POSITION_RAW value"))
        if row.get("FASTING_RAW"):
            fast_val = fast_lut.get(str(row["FASTING_RAW"]).strip().upper())
            if fast_val:
                out["VSFAST"] = fast_val
            else:
                row_issues.append(issue("VS_FAST_001", "Unsupported FASTING_RAW value"))

        if out["VSORRES"] is None:
            row_issues.append(issue("VS_ORRES_001", "Missing VSORRES"))

        num = parse_num(out["VSORRES"])
        if out["VSTESTCD"] is not None and out["VSTEST"] is not None:
            if num is None:
                row_issues.append(issue("VS_STRES_001", "Non-numeric result for numeric VS test"))
            else:
                if out["VSORRESU"] is None:
                    row_issues.append(issue("VS_STRES_002", "Cannot standardize without supported normalized original unit"))
                elif unit_info is None:
                    row_issues.append(issue("VS_STRES_003", "Unit mapping unavailable for standardization"))
                else:
                    std_num = apply_conversion(out["VSTESTCD"], num, unit_info["CONVERSION_RULE"], conv_lut)
                    if std_num is None:
                        row_issues.append(issue("VS_STRES_004", "Conversion rule missing or failed"))
                    else:
                        out["VSSTRESN"] = std_num
                        out["VSSTRESC"] = str(std_num)
                        out["VSSTRESU"] = unit_info["VSSTRESU"]
                        thresh = impl_lut.get(out["VSTESTCD"])
                        if thresh and not (thresh["min"] <= std_num <= thresh["max"]):
                            row_issues.append(issue("VS_QC_001", "Standardized value outside plausibility threshold"))

        if out["VSDTC"] and is_partial_date(out["VSDTC"]) and not ALLOW_PARTIAL_VSDTC_IN_FINAL:
            row_issues.append(issue("VS_DATE_001", "Partial VSDTC routed to exception by sponsor/demo policy"))

        for req in ["STUDYID", "DOMAIN", "USUBJID", "VSDTC", "VSTESTCD", "VSTEST", "VSORRES"]:
            if not out.get(req):
                row_issues.append(issue("VS_REQ_001", f"{req} missing"))

        rec = {c: out.get(c) for c in SUBMISSION_COLS}
        rec["SOURCE_ROW_NUMBER"] = str(src_row_num)
        rec["ROW_ISSUES"] = " | ".join(x["issue_message"] for x in row_issues)
        rec["ISSUE_CODES"] = " | ".join(x["issue_code"] for x in row_issues)

        hard_fail = any(x["severity"] == "ERROR" for x in row_issues)
        if hard_fail:
            exc_rows.append(rec)
            disposition = "EXCEPTION"
        elif row_issues:
            pass_rows.append(rec)
            disposition = "PASS_WITH_WARNINGS"
        else:
            pass_rows.append(rec)
            disposition = "PASS"

        qc_rows.append({
            "SOURCE_ROW_NUMBER": str(src_row_num),
            "USUBJID": out.get("USUBJID"),
            "VSTESTCD": out.get("VSTESTCD"),
            "ISSUE_COUNT": len(row_issues),
            "ISSUE_CODES": " | ".join(x["issue_code"] for x in row_issues),
            "ISSUES": " | ".join(x["issue_message"] for x in row_issues),
            "DISPOSITION": disposition
        })

    final_internal = pd.DataFrame(pass_rows)
    exceptions_internal = pd.DataFrame(exc_rows)
    qc_df = pd.DataFrame(qc_rows)

    final_internal = assign_vsseq(final_internal)
    exceptions_internal = assign_vsseq(exceptions_internal)

    if not final_internal.empty:
        dup_key = ["STUDYID", "USUBJID", "VSTESTCD", "VSDTC", "VISITNUM"]
        dup_mask = final_internal.duplicated(subset=dup_key, keep=False)
        if dup_mask.any():
            dup_rows = final_internal.loc[dup_mask].copy()
            dup_rows["ROW_ISSUES"] = dup_rows["ROW_ISSUES"].fillna("") + " | Duplicate final VS operational key"
            dup_rows["ISSUE_CODES"] = dup_rows["ISSUE_CODES"].fillna("") + " | VS_DUP_001"
            dup_source_rows = set(dup_rows["SOURCE_ROW_NUMBER"].astype(str).tolist())
            qc_df.loc[qc_df["SOURCE_ROW_NUMBER"].astype(str).isin(dup_source_rows), "DISPOSITION"] = "EXCEPTION"
            qc_df.loc[qc_df["SOURCE_ROW_NUMBER"].astype(str).isin(dup_source_rows), "ISSUE_CODES"] = qc_df.loc[
                qc_df["SOURCE_ROW_NUMBER"].astype(str).isin(dup_source_rows), "ISSUE_CODES"
            ].fillna("").apply(lambda x: (x + " | VS_DUP_001").strip(" |"))
            qc_df.loc[qc_df["SOURCE_ROW_NUMBER"].astype(str).isin(dup_source_rows), "ISSUES"] = qc_df.loc[
                qc_df["SOURCE_ROW_NUMBER"].astype(str).isin(dup_source_rows), "ISSUES"
            ].fillna("").apply(lambda x: (x + " | Duplicate final VS operational key").strip(" |"))
            exceptions_internal = pd.concat([exceptions_internal, dup_rows], ignore_index=True)
            final_internal = final_internal.loc[~dup_mask].copy()
            final_internal = assign_vsseq(final_internal)
            exceptions_internal = assign_vsseq(exceptions_internal)

    accounted = set(final_internal["SOURCE_ROW_NUMBER"].astype(str).tolist()) | set(exceptions_internal["SOURCE_ROW_NUMBER"].astype(str).tolist())
    missing_rows = sorted(all_source_rows - accounted)
    no_loss_df = pd.DataFrame({"missing_source_row_number": missing_rows})
    if missing_rows:
        raise RuntimeError(f"No-data-loss check failed; missing rows in outputs: {missing_rows[:10]}")

    final_internal = enforce_output_types(final_internal)
    exceptions_internal = enforce_output_types(exceptions_internal)

    final_submission = final_internal[SUBMISSION_COLS].copy() if not final_internal.empty else pd.DataFrame(columns=SUBMISSION_COLS)
    exceptions_submission = exceptions_internal[SUBMISSION_COLS].copy() if not exceptions_internal.empty else pd.DataFrame(columns=SUBMISSION_COLS)

    final_internal.to_csv(OUTDIR / "vs_sdtm_final_internal_v4.csv", index=False)
    exceptions_internal.to_csv(OUTDIR / "vs_sdtm_exceptions_internal_v4.csv", index=False)
    final_submission.to_csv(OUTDIR / "vs_sdtm_final_submission_v4.csv", index=False)
    exceptions_submission.to_csv(OUTDIR / "vs_sdtm_exceptions_submission_v4.csv", index=False)
    qc_df.to_csv(OUTDIR / "vs_sdtm_qc_report_v4.csv", index=False)
    no_loss_df.to_csv(OUTDIR / "vs_no_data_loss_check_v4.csv", index=False)

    summary = pd.DataFrame([{
        "layer1_source_used": str(source_path),
        "input_rows": len(df),
        "final_internal_rows": len(final_internal),
        "exceptions_internal_rows": len(exceptions_internal),
        "qc_rows": len(qc_df),
        "missing_rows_after_no_loss_check": len(missing_rows),
        "partial_vsdtc_allowed_in_final": ALLOW_PARTIAL_VSDTC_IN_FINAL,
        "vscat_included": False,
        "vsstat_included": False,
        "vsorres_forced_character": True,
        "vsstresc_forced_character": True,
        "vsstresn_kept_numeric": True
    }])
    summary.to_csv(OUTDIR / "vs_build_summary_v4.csv", index=False)

    readme = (
        "VS SDTM v4 build\n"
        f"- Layer 1 source used: {source_path}\n"
        "- Keeps current spec unchanged (no VSCAT or VSSTAT added)\n"
        "- VSORRES is explicitly preserved as character\n"
        "- VSSTRESC is explicitly preserved as character\n"
        "- VSSTRESN is explicitly kept numeric\n"
        "- Includes VSSEQ in both final and exceptions datasets\n"
        "- Produces internal audit outputs and clean submission-style outputs\n"
        "- No-data-loss policy enforced: every Layer 1 row must end in final or exceptions\n"
        f"- Partial VSDTC allowed in final: {ALLOW_PARTIAL_VSDTC_IN_FINAL}\n"
    )
    (OUTDIR / "README.txt").write_text(readme, encoding="utf-8")
    print(f"Created outputs in: {OUTDIR}")

if __name__ == "__main__":
    main()
