from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List

import pandas as pd

DOMAIN_RULES: Dict[str, List[str]] = {
    "DM": [
        "STUDYID", "USUBJID", "SUBJID", "SITEID", "SEX", "RACE", "ETHNIC", "BRTHDTC", "RFSTDTC",
        "STUDY NUMBER", "SITE NO", "SCREENING NO", "UNIQUE SUBJECT ID", "DATE OF BIRTH", "SEX AT BIRTH",
        "RACE CATEGORY", "ETHNIC GROUP", "INFORMED CONSENT DT", "FIRST DOSE DATE", "ASSIGNED TREATMENT",
        "COUNTRY OF SITE", "SCREEN FAILURE FLAG",
    ],
    "VS": [
        "VSTEST", "VS_DATE", "VISIT_NAME", "VSORRES", "VSORRESU", "POSITION", "FASTING",
        "VS_TEST_RAW", "VS_RESULT_RAW", "VS_UNIT_RAW", "VISIT_NUM", "VS_TIME", "SUBJECT_KEY",
    ],
    "LB": [
        "LBTEST", "LBORRES", "LBORRESU", "LBSPEC", "LBDTC", "VISIT",
        "TEST_CODE_RAW", "TEST_NAME_RAW", "RESULT_RAW", "ORIG_UNIT_RAW", "SPECIMEN_RAW", "COLLECTION_DATE_RAW",
    ],
    "AE": [
        "AETERM", "AE_TERM", "AE_START_DATE_RAW", "AE_SEVERITY_RAW", "AEYN_RAW", "AE_OUTCOME_RAW",
        "AE_SER_RAW", "AE_ONGOING_RAW", "AE_REL_STUDY_DRUG_RAW", "VISITDT_RAW", "ENTRY_STATUS_RAW",
    ],
}


def _read_headers(file_bytes: bytes, filename: str) -> List[str]:
    lower = filename.lower()
    if lower.endswith(".csv"):
        df = pd.read_csv(BytesIO(file_bytes), dtype=str, nrows=0)
    else:
        df = pd.read_excel(BytesIO(file_bytes), dtype=str, nrows=0)
    return [str(c).strip().upper() for c in df.columns]


def detect_domain(file_bytes: bytes, filename: str) -> Dict[str, Any]:
    headers = _read_headers(file_bytes, filename)
    score_map: Dict[str, int] = {}
    matched: Dict[str, List[str]] = {}
    for domain, rules in DOMAIN_RULES.items():
        matches = [col for col in rules if col.upper() in headers]
        score_map[domain] = len(matches)
        matched[domain] = matches

    best_domain = max(score_map, key=score_map.get)
    total = max(len(DOMAIN_RULES[best_domain]), 1)
    confidence = score_map[best_domain] / total

    sorted_scores = sorted(score_map.items(), key=lambda item: item[1], reverse=True)
    runner_up = sorted_scores[1][0] if len(sorted_scores) > 1 else None
    return {
        "domain": best_domain,
        "confidence": round(confidence, 3),
        "matched_columns": matched[best_domain],
        "all_scores": score_map,
        "runner_up": runner_up,
        "headers": headers,
    }
