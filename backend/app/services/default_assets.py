from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import pandas as pd


def ensure_dm_support_assets(target_dir: Path) -> None:
    pd.DataFrame([
        {"codelist_name": "SEX", "source_value": "M", "standard_value": "M"},
        {"codelist_name": "SEX", "source_value": "F", "standard_value": "F"},
        {"codelist_name": "SEX", "source_value": "U", "standard_value": "U"},
        {"codelist_name": "DTHFL", "source_value": "Y", "standard_value": "Y"},
        {"codelist_name": "DTHFL", "source_value": "N", "standard_value": "N"},
    ]).to_csv(target_dir / "controlled_terminology_demo.csv", index=False)

    (target_dir / "study_metadata_demo.json").write_text(json.dumps({"studyid": "DEMO-STUDY"}, indent=2), encoding="utf-8")
    (target_dir / "programming_conventions_demo.json").write_text(json.dumps({"missing_values": ["", "NA", "NULL", "NAN", "NONE"]}, indent=2), encoding="utf-8")
    (target_dir / "demo_sponsor_rules_dm.json").write_text(json.dumps({"allowed_intentional_loss_fields": ["DTHFL"]}, indent=2), encoding="utf-8")


def ensure_vs_rules(target_path: Path) -> None:
    payload: Dict[str, object] = {
        "source_required_fields": ["STUDYID", "USUBJID", "VISIT_NAME", "VISIT_NUM", "VS_DATE", "TEST_NAME", "ORIG_RESULT", "ORIG_UNIT"],
        "test_name_rules": {"allowed_raw_test_names": ["SYSTOLIC BLOOD PRESSURE", "DIASTOLIC BLOOD PRESSURE", "PULSE RATE", "TEMPERATURE", "WEIGHT", "HEIGHT"]},
        "unit_rules": {
            "allowed_source_units_by_test": {
                "SYSTOLIC BLOOD PRESSURE": ["MMHG"],
                "DIASTOLIC BLOOD PRESSURE": ["MMHG"],
                "PULSE RATE": ["BPM"],
                "TEMPERATURE": ["C", "F", "K"],
                "WEIGHT": ["KG", "LB", "LBS"],
                "HEIGHT": ["CM", "IN"]
            },
            "unit_required_for_numeric_tests": True,
            "also_flag_unit_normalization_issue_when_missing": True
        },
        "numeric_result_rules": {
            "numeric_tests": ["SYSTOLIC BLOOD PRESSURE", "DIASTOLIC BLOOD PRESSURE", "PULSE RATE", "TEMPERATURE", "WEIGHT", "HEIGHT"],
            "allow_negative_values_for_tests": [],
            "implausibility_thresholds": {
                "SYSTOLIC BLOOD PRESSURE": {"min": 40, "max": 300},
                "DIASTOLIC BLOOD PRESSURE": {"min": 20, "max": 200},
                "PULSE RATE": {"min": 20, "max": 250},
                "TEMPERATURE_C_EQUIV": {"min": 30, "max": 45},
                "WEIGHT_KG_EQUIV": {"min": 2, "max": 400},
                "HEIGHT_CM_EQUIV": {"min": 20, "max": 260}
            }
        },
        "visit_rules": {"expected_visitnum_by_visitname": {"SCREENING": "1", "BASELINE": "2", "WEEK 1": "3", "WEEK 2": "4", "END OF STUDY": "99"}},
        "position_rules": {"allowed_values": ["SITTING", "SUPINE", "STANDING"]},
        "fasting_rules": {"allowed_values": ["Y", "N"]},
        "duplicate_rules": {"candidate_key_fields": ["USUBJID", "VISIT_NAME", "TEST_NAME", "VS_DATE"]},
    }
    target_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def ensure_ae_rules(target_path: Path) -> None:
    rules = {}
    for i in range(1, 40):
        rid = f"AE{i:03d}"
        bucket = "SDTM_STANDARDISABLE" if rid in {"AE008", "AE010", "AE016", "AE018", "AE032", "AE037"} else "Human"
        severity = "WARNING" if bucket == "SDTM_STANDARDISABLE" else "ERROR"
        rules[rid] = {"severity": severity, "bucket": bucket, "description": rid, "basis": "Demo rule metadata"}
    payload = {
        "input": {"source_csv": "ae_raw.csv"},
        "required_fields_always": ["ROW_ID", "STUDYID_RAW", "SITEID_RAW", "SUBJECT_RAW", "AEYN_RAW"],
        "required_when_ae_present": ["AE_TERM", "AE_START_DATE_RAW", "AE_SEVERITY_RAW", "AE_SER_RAW", "AE_OUTCOME_RAW", "AE_REL_STUDY_DRUG_RAW", "AE_ONGOING_RAW"],
        "allowed": {
            "yes_no": ["YES", "NO"],
            "severity": ["MILD", "MODERATE", "SEVERE"],
            "toxgr": ["1", "2", "3", "4", "5"],
            "relationship": ["NOT RELATED", "UNLIKELY", "POSSIBLE", "PROBABLE", "RELATED"],
            "prespecified": ["YES", "NO"],
            "reporter": ["INVESTIGATOR", "SUBJECT", "CAREGIVER", "OTHER"],
            "entry_status": ["NEW", "UPDATED", "UNCHANGED"],
            "outcome": ["RECOVERED/RESOLVED", "RECOVERING/RESOLVING", "NOT RECOVERED/NOT RESOLVED", "RECOVERED/RESOLVED WITH SEQUELAE", "FATAL", "UNKNOWN"]
        },
        "severity_toxgr_map": {"MILD": ["1", "2"], "MODERATE": ["2", "3"], "SEVERE": ["3", "4", "5"]},
        "rules": rules,
        "suppression": {"by_rule": {}, "priority": {f"AE{i:03d}": i for i in range(1, 40)}},
    }
    target_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
