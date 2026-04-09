from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd


@dataclass
class DetectionResult:
    domain: Optional[str]
    confidence: int
    matched_columns: List[str]
    scores: Dict[str, int]


DOMAIN_RULES: Dict[str, Dict[str, Set[str]]] = {
    "DM": {
        "strong": {
            "STUDYID",
            "SITEID",
            "SUBJID",
            "SEX",
            "AGE",
            "AGEU",
            "RACE",
            "ETHNIC",
            "COUNTRY",
            "ARM",
            "ACTARM",
            "RFSTDTC",
            "BRTHDTC",
            "DTHFL",
            "DTHDTC",
        },
        "weak": {
            "SUBJECT_KEY",
            "USUBJID",
            "SITE_NO",
            "SUBJECT_NO",
            "SCREENING_NO",
            "RAND_NO",
        },
    },
    "AE": {
        "strong": {
            "AE_TERM",
            "AE_START_DATE_RAW",
            "AE_SEVERITY_RAW",
            "AEYN_RAW",
            "AE_OUTCOME_RAW",
            "AE_SER_RAW",
            "AE_ONGOING_RAW",
            "AE_REL_STUDY_DRUG_RAW",
            "VISITDT_RAW",
            "ENTRY_STATUS_RAW",
        },
        "weak": {
            "AE_TOXGR_RAW",
            "AE_START_TIME_RAW",
            "AE_END_DATE_RAW",
            "AE_END_TIME_RAW",
            "AE_FORM_SEQ",
            "AE_SEQ_CRF",
            "AE_SEQ_CRF".replace("_CRF", "_CRf"),  # keeps compatibility with your existing raw files
            "AE_COMMENT",
        },
    },
    "LB": {
        "strong": {
            "TEST_CODE_RAW",
            "TEST_NAME_RAW",
            "RESULT_RAW",
            "ORIG_UNIT_RAW",
            "SPECIMEN_RAW",
            "RESULT_NUM_RAW",
            "ABN_FLAG_RAW",
            "NOT_DONE_RAW",
        },
        "weak": {
            "REF_LOW_RAW",
            "REF_HIGH_RAW",
            "REF_UNIT_RAW",
            "COMMENT_RAW",
            "VISITDT_RAW",
            "COLL_DATE_RAW",
            "COLL_TIME_RAW",
        },
    },
    "VS": {
        "strong": {
            "VS_TEST_RAW",
            "VS_RESULT_RAW",
            "VS_UNIT_RAW",
            "VS_DATE",
            "VISIT_NAME",
            "VISIT_NUM",
        },
        "weak": {
            "POSITION_RAW",
            "FASTING_RAW",
            "SUBJECT_KEY",
            "VS_TIME",
            "PROTOCOL_ID",
            "SITE_NUMBER",
            "SUBJECT_NUMBER",
        },
    },
}


COLUMN_ALIASES: Dict[str, str] = {
    "AE_SEQ_CRF": "AE_SEQ_CRF",
    "AE_SEQ_CRF ".strip(): "AE_SEQ_CRF",
    "AE_SEQ_CRF": "AE_SEQ_CRF",
    "AE_SEQ_CRF".replace("_CRF", "_CRf"): "AE_SEQ_CRF",
    "VISITNUM": "VISIT_NUM",
    "VISITNUM_RAW": "VISIT_NUM",
    "VISIT_NUM_RAW": "VISIT_NUM",
    "SITE_NO": "SITE_NO",
    "SUBJECT_NO": "SUBJECT_NO",
}


def _normalize_columns(columns: Iterable[str]) -> Set[str]:
    normalized: Set[str] = set()
    for col in columns:
        c = str(col).strip().upper()
        c = COLUMN_ALIASES.get(c, c)
        normalized.add(c)
    return normalized


def _matched_columns_for_domain(domain: str, columns: Set[str]) -> List[str]:
    rules = DOMAIN_RULES[domain]
    matches = sorted((columns & rules["strong"]) | (columns & rules["weak"]))
    return matches


def detect_domain_from_columns(columns: Iterable[str]) -> DetectionResult:
    cols = _normalize_columns(columns)

    scores: Dict[str, int] = {}
    strong_hit_counts: Dict[str, int] = {}

    for domain, rules in DOMAIN_RULES.items():
        strong_hits = cols & rules["strong"]
        weak_hits = cols & rules["weak"]

        strong_hit_counts[domain] = len(strong_hits)
        scores[domain] = len(strong_hits) * 3 + len(weak_hits)

    best_domain = max(scores, key=scores.get)
    best_score = scores[best_domain]

    # Require at least one strong domain-specific signal.
    if strong_hit_counts[best_domain] == 0 or best_score == 0:
        return DetectionResult(
            domain=None,
            confidence=0,
            matched_columns=[],
            scores=scores,
        )

    total_score = sum(scores.values())
    confidence = int(round((best_score / total_score) * 100)) if total_score > 0 else 0

    return DetectionResult(
        domain=best_domain,
        confidence=confidence,
        matched_columns=_matched_columns_for_domain(best_domain, cols),
        scores=scores,
    )


def detect_domain(value) -> DetectionResult:
    if isinstance(value, pd.DataFrame):
        return detect_domain_from_dataframe(value)
    return detect_domain_from_columns(value)
