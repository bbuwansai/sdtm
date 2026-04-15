import argparse
import json
import re
from pathlib import Path
from typing import Optional
import pandas as pd

BASE = Path(__file__).resolve().parent
DEFAULT_SOURCE = BASE / "vs_raw_crf_style_demo.csv"
DEFAULT_RULES = BASE / "vs_layer1_rules_v4.json"
DEFAULT_OUTDIR = BASE / "vs_layer1_outputs_v4"

HUMAN_RULE_IDS = {
    "VS001", "VS002", "VS003", "VS004", "VS005", "VS006", "VS008", "VS011",
    "VS022", "VS023", "VS024", "VS025", "VS026", "VS027"
}
SDTM_RULE_IDS = {
    "VS007", "VS009", "VS010", "VS012", "VS013", "VS014", "VS015",
    "VS016", "VS017", "VS018", "VS019", "VS020", "VS021"
}


def classify_bucket(rule_id: str, severity: str) -> str:
    if rule_id in SDTM_RULE_IDS:
        return "SDTM_STANDARDISABLE"
    if rule_id in HUMAN_RULE_IDS:
        return "Human"
    return "SDTM_STANDARDISABLE" if severity == "WARNING" else "Human"

def clean(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    return None if s.upper() in {"", "NA", "NULL", "NAN", "NONE"} else s

def to_iso_partial_date(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if re.fullmatch(r"\d{4}", s) or re.fullmatch(r"\d{4}-\d{2}", s) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    return None

def valid_time_hhmm(v: Optional[str]) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    if not re.fullmatch(r"\d{2}:\d{2}", s):
        return False
    hh, mm = int(s[:2]), int(s[3:])
    return 0 <= hh <= 23 and 0 <= mm <= 59

def parse_num(v: Optional[str]):
    if v is None:
        return None
    s = str(v).strip()
    if re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", s):
        return float(s)
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

def celsius_equiv(raw_num, unit):
    if raw_num is None or unit is None:
        return None
    u = str(unit).strip().upper()
    if u in {"C", "CELSIUS"}:
        return raw_num
    if u in {"F", "FAHRENHEIT"}:
        return (raw_num - 32.0) * 5.0 / 9.0
    if u == "K":
        return raw_num - 273.15
    return None

def kg_equiv(raw_num, unit):
    if raw_num is None or unit is None:
        return None
    u = str(unit).strip().upper()
    if u in {"KG", "KILOGRAMS"}:
        return raw_num
    if u in {"LB", "LBS"}:
        return raw_num * 0.45359237
    return None

def cm_equiv(raw_num, unit):
    if raw_num is None or unit is None:
        return None
    u = str(unit).strip().upper()
    if u == "CM":
        return raw_num
    if u in {"IN", "INCH"}:
        return raw_num * 2.54
    return None

def main():
    parser = argparse.ArgumentParser(description="VS Layer 1 QC v4")
    parser.add_argument("--source", help="Path to VS raw CSV input")
    parser.add_argument("--rules", help="Path to rules JSON")
    parser.add_argument("--outdir", help="Optional output directory override")
    args = parser.parse_args()

    source = Path(args.source).resolve() if args.source else DEFAULT_SOURCE
    rules_path = Path(args.rules).resolve() if args.rules else DEFAULT_RULES
    outdir = Path(args.outdir).resolve() if args.outdir else DEFAULT_OUTDIR

    outdir.mkdir(parents=True, exist_ok=True)
    rules = json.loads(rules_path.read_text(encoding="utf-8"))
    df = pd.read_csv(source, dtype=str)
    for c in df.columns:
        df[c] = df[c].apply(clean)

    df["VISIT_NUM"] = df["VISIT_NUM"].apply(lambda x: None if x is None else str(int(float(x))) if str(x).replace('.','',1).isdigit() else x)
    df["VS_DATE"] = df["VS_DATE"].apply(to_iso_partial_date)
    df.insert(0, "L1_SOURCE_ROW_NUMBER", range(1, len(df) + 1))

    issues = []
    summary = {}

    def add_issue(rule_id, desc, idx=None, severity="ERROR", subject_key=None, visit_name=None, visit_num=None, missing_expected_test=None):
        src_row_number = None if idx is None else int(df.at[idx, "L1_SOURCE_ROW_NUMBER"])
        bucket = classify_bucket(rule_id, severity)
        issues.append({
            "source_row_number": src_row_number,
            "rule_id": rule_id,
            "severity": severity,
            "final_bucket": bucket,
            "rule_description": desc,
            "message": desc,
            "subject_key": subject_key,
            "visit_name": visit_name,
            "visit_num": visit_num,
            "missing_expected_test": missing_expected_test
        })
        summary[(rule_id, severity, bucket, desc)] = summary.get((rule_id, severity, bucket, desc), 0) + 1

    allowed_tests = set(rules["test_name_rules"]["allowed_raw_test_names"])
    allowed_units = rules["unit_rules"]["allowed_source_units_by_test"]
    numeric_tests = set(rules["numeric_result_rules"]["numeric_tests"])
    expected_visitnum = rules["visit_rules"]["expected_visitnum_by_visitname"]
    allowed_positions = set(rules["position_rules"]["allowed_values"])
    allowed_fasting = set(rules["fasting_rules"]["allowed_values"])

    for idx, row in df.iterrows():
        for req in rules["source_required_fields"]:
            if not row.get(req):
                add_issue("VS001", f"{req} missing", idx)

        if row.get("VS_DATE") is None:
            add_issue("VS002", "VS_DATE missing or not in allowed partial/full ISO format", idx)

        if not valid_time_hhmm(row.get("VS_TIME")):
            add_issue("VS003", "VS_TIME not in allowed HH:MM format", idx)

        test_name = row.get("VS_TEST_RAW")
        unit = row.get("VS_UNIT_RAW")
        result = row.get("VS_RESULT_RAW")
        visit_name = row.get("VISIT_NAME")
        visit_num = row.get("VISIT_NUM")
        position = row.get("POSITION_RAW")
        fasting = row.get("FASTING_RAW")

        if visit_name in expected_visitnum and visit_num is not None and str(visit_num) != str(expected_visitnum[visit_name]):
            add_issue("VS004", "VISIT_NAME and VISIT_NUM inconsistent with sponsor visit map", idx)

        if test_name and test_name not in allowed_tests:
            add_issue("VS005", "VS_TEST_RAW not in allowed raw test-name list", idx)

        if test_name in allowed_units:
            if rules["unit_rules"]["unit_required_for_numeric_tests"] and not unit:
                add_issue("VS006", "VS_UNIT_RAW missing for numeric VS test", idx)
                if rules["unit_rules"]["also_flag_unit_normalization_issue_when_missing"]:
                    add_issue("VS007", "VS unit unavailable for normalization/standardization checks", idx)
            elif unit and unit not in allowed_units[test_name]:
                add_issue("VS008", "VS_UNIT_RAW not allowed for VS_TEST_RAW", idx)

        if position is not None and position not in allowed_positions:
            add_issue("VS009", "POSITION_RAW not in allowed value set", idx)

        if fasting is not None and fasting not in allowed_fasting:
            add_issue("VS010", "FASTING_RAW not in allowed value set", idx)

        if test_name in numeric_tests:
            num = parse_num(result)
            if num is None:
                add_issue("VS011", "VS_RESULT_RAW non-numeric for expected numeric VS test", idx)
            else:
                if test_name not in rules["numeric_result_rules"]["allow_negative_values_for_tests"] and num < 0:
                    add_issue("VS012", "Negative VS_RESULT_RAW not allowed for this VS test", idx)

                if test_name in {"SYSTOLIC BLOOD PRESSURE", "SYST BP"}:
                    th = rules["numeric_result_rules"]["implausibility_thresholds"]["SYSTOLIC BLOOD PRESSURE"]
                    if not (th["min"] <= num <= th["max"]):
                        add_issue("VS013", "Systolic BP outside plausibility range", idx, "WARNING")
                elif test_name == "DIASTOLIC BLOOD PRESSURE":
                    th = rules["numeric_result_rules"]["implausibility_thresholds"]["DIASTOLIC BLOOD PRESSURE"]
                    if not (th["min"] <= num <= th["max"]):
                        add_issue("VS014", "Diastolic BP outside plausibility range", idx, "WARNING")
                elif test_name == "PULSE RATE":
                    th = rules["numeric_result_rules"]["implausibility_thresholds"]["PULSE RATE"]
                    if not (th["min"] <= num <= th["max"]):
                        add_issue("VS015", "Pulse outside plausibility range", idx, "WARNING")
                elif test_name == "TEMPERATURE":
                    c = celsius_equiv(num, unit)
                    if c is None:
                        add_issue("VS016", "Temperature unit unsupported for normalized plausibility check", idx)
                    else:
                        th = rules["numeric_result_rules"]["implausibility_thresholds"]["TEMPERATURE_C_EQUIV"]
                        if not (th["min"] <= c <= th["max"]):
                            add_issue("VS017", "Temperature outside plausibility range after unit normalization", idx, "WARNING")
                elif test_name == "WEIGHT":
                    kg = kg_equiv(num, unit)
                    if kg is None:
                        add_issue("VS018", "Weight unit unsupported for normalized plausibility check", idx)
                    else:
                        th = rules["numeric_result_rules"]["implausibility_thresholds"]["WEIGHT_KG_EQUIV"]
                        if not (th["min"] <= kg <= th["max"]):
                            add_issue("VS019", "Weight outside plausibility range after unit normalization", idx, "WARNING")
                elif test_name == "HEIGHT":
                    cm = cm_equiv(num, unit)
                    if cm is None:
                        add_issue("VS020", "Height unit unsupported for normalized plausibility check", idx)
                    else:
                        th = rules["numeric_result_rules"]["implausibility_thresholds"]["HEIGHT_CM_EQUIV"]
                        if not (th["min"] <= cm <= th["max"]):
                            add_issue("VS021", "Height outside plausibility range after unit normalization", idx, "WARNING")

    dup_mask = df.duplicated(subset=rules["duplicate_rules"]["candidate_key_fields"], keep=False)
    for idx in df.index[dup_mask]:
        add_issue("VS022", "Potential duplicate VS record on candidate source key", idx)

    for subject_key, sdf in df.groupby("SUBJECT_KEY", dropna=False):
        if subject_key is None:
            continue
        tmp = sdf[["L1_SOURCE_ROW_NUMBER", "VISIT_NAME", "VISIT_NUM", "VS_DATE"]].drop_duplicates().copy()
        tmp["VISIT_NUM_N"] = pd.to_numeric(tmp["VISIT_NUM"], errors="coerce")
        tmp["DATE_KEY"] = tmp["VS_DATE"].apply(partial_sort_key)
        tmp = tmp.sort_values(["VISIT_NUM_N", "DATE_KEY"], na_position="last")

        prev_date = None
        prev_visitnum = None
        for _, r in tmp.iterrows():
            visitnum = r["VISIT_NUM_N"]
            date_key = r["DATE_KEY"]
            if prev_visitnum is not None and visitnum is not None and date_key is not None and prev_date is not None and visitnum > prev_visitnum and date_key < prev_date:
                src_row = int(r["L1_SOURCE_ROW_NUMBER"]) - 1
                add_issue("VS023", "Later visit occurs before earlier visit date", src_row)
            prev_visitnum = visitnum if visitnum is not None else prev_visitnum
            prev_date = date_key if date_key is not None else prev_date

        scr = tmp[tmp["VISIT_NAME"] == "SCREENING"]["DATE_KEY"].dropna().tolist()
        base = tmp[tmp["VISIT_NAME"] == "BASELINE"]["DATE_KEY"].dropna().tolist()
        post = tmp[tmp["VISIT_NUM_N"] > 20]["DATE_KEY"].dropna().tolist()
        if scr and base and min(base) < min(scr):
            src_row = int(tmp[tmp["VISIT_NAME"] == "BASELINE"]["L1_SOURCE_ROW_NUMBER"].iloc[0]) - 1
            add_issue("VS024", "BASELINE occurs before SCREENING", src_row)
        if base and post and min(post) < min(base):
            src_row = int(tmp[tmp["VISIT_NUM_N"] > 20]["L1_SOURCE_ROW_NUMBER"].iloc[0]) - 1
            add_issue("VS025", "Post-baseline visit occurs before BASELINE", src_row)

    panel_cfg = rules.get("panel_completeness_rules", {})
    if panel_cfg.get("enabled", False):
        panel = pd.read_csv(BASE / panel_cfg["expected_panel_file"], dtype=str)
        for c in panel.columns:
            panel[c] = panel[c].apply(clean)

        expected_tests_by_visit = {}
        for _, r in panel.iterrows():
            key = (str(r["visit_name"]).strip(), str(r["visitnum"]).strip())
            expected_tests_by_visit.setdefault(key, set()).add(str(r["expected_test_name"]).strip())

        if panel_cfg.get("flag_subject_row_count_mismatch", False):
            expected_total = int(panel_cfg.get("expected_rows_per_subject", 0))
            obs_counts = df.groupby("SUBJECT_KEY", dropna=False).size().to_dict()
            for subject_key, obs_count in obs_counts.items():
                if subject_key is not None and expected_total and int(obs_count) != expected_total:
                    add_issue("VS026", f"Subject does not have expected total row count of {expected_total}", idx=None, subject_key=subject_key)

        anchor = (
            df.groupby(["SUBJECT_KEY", "VISIT_NAME", "VISIT_NUM"], dropna=False)["L1_SOURCE_ROW_NUMBER"]
            .min().reset_index()
        )
        anchor_lut = {
            (r["SUBJECT_KEY"], r["VISIT_NAME"], r["VISIT_NUM"]): int(r["L1_SOURCE_ROW_NUMBER"])
            for _, r in anchor.iterrows()
        }

        observed = (
            df.groupby(["SUBJECT_KEY", "VISIT_NAME", "VISIT_NUM"], dropna=False)["VS_TEST_RAW"]
            .apply(lambda s: set(str(x).strip() for x in s.dropna().tolist()))
            .to_dict()
        )

        subjects = [s for s in df["SUBJECT_KEY"].dropna().unique().tolist()]
        for subject_key in subjects:
            for (visit_name, visitnum), expected_tests in expected_tests_by_visit.items():
                obs_tests = observed.get((subject_key, visit_name, visitnum), set())
                missing_tests = sorted(expected_tests - obs_tests)
                anchor_row = anchor_lut.get((subject_key, visit_name, visitnum))
                for mt in missing_tests:
                    add_issue(
                        "VS027",
                        "Expected test row missing for subject/visit/test panel",
                        idx=(anchor_row - 1) if anchor_row is not None else None,
                        subject_key=subject_key,
                        visit_name=visit_name,
                        visit_num=visitnum,
                        missing_expected_test=mt
                    )

    issue_df = pd.DataFrame(issues)
    if not issue_df.empty:
        issue_df = issue_df.sort_values(
            ["subject_key", "visit_name", "visit_num", "missing_expected_test", "source_row_number", "rule_id"],
            na_position="last"
        ).reset_index(drop=True)

    summary_df = pd.DataFrame([
        {"rule_id": rid, "severity": sev, "final_bucket": bucket, "rule_description": desc, "issue_count": count}
        for (rid, sev, bucket, desc), count in summary.items()
    ]).sort_values(["rule_id", "severity", "final_bucket"]).reset_index(drop=True)

    df.to_csv(outdir / "vs_cleaned_output_v4.csv", index=False)
    df.to_csv(outdir / "vs_cleaned_output.csv", index=False)
    issue_df.to_csv(outdir / "vs_issue_log_v4.csv", index=False)
    issue_df.to_csv(outdir / "vs_issue_log_all_v4.csv", index=False)
    issue_df.to_csv(outdir / "vs_issue_log_all.csv", index=False)
    issue_df[issue_df["final_bucket"] == "Human"].to_csv(outdir / "vs_issue_log_human_v4.csv", index=False)
    issue_df[issue_df["final_bucket"] == "Human"].to_csv(outdir / "vs_issue_log_human.csv", index=False)
    issue_df[issue_df["final_bucket"] == "SDTM_STANDARDISABLE"].to_csv(outdir / "vs_issue_log_sdtm_standardisable_v4.csv", index=False)
    issue_df[issue_df["final_bucket"] == "SDTM_STANDARDISABLE"].to_csv(outdir / "vs_issue_log_sdtm_standardisable.csv", index=False)
    summary_df.to_csv(outdir / "vs_issue_summary_by_rule_v4.csv", index=False)
    summary_df.to_csv(outdir / "vs_issue_summary_by_rule.csv", index=False)
    print(f"Created outputs in: {outdir}")

if __name__ == "__main__":
    main()
