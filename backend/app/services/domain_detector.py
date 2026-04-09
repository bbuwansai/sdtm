from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd


DOMAIN_RULES: Dict[str, Dict[str, Set[str]]] = {
    "DM": {
        "strong": {
            "STUDYID", "SITEID", "SUBJID", "SEX", "AGE", "AGEU", "RACE", "ETHNIC",
            "COUNTRY", "ARM", "ACTARM", "RFSTDTC", "BRTHDTC", "DTHFL", "DTHDTC",
            "SITE_NO", "SUBJECT_NO", "SCREENING_NO", "RAND_NO",
            "STUDY_NUMBER", "SITE_NUMBER", "UNIQUE_SUBJECT_ID", "DATE_OF_BIRTH",
            "SEX_AT_BIRTH", "RACE_CATEGORY", "ETHNIC_GROUP", "FIRST_DOSE_DATE",
            "ASSIGNED_TREATMENT", "COUNTRY_OF_SITE", "SCREEN_FAILURE_FLAG",
        },
        "weak": {
            "SUBJECT_KEY", "USUBJID", "INFORMED_CONSENT_DT", "INVESTIGATOR_NAME",
        },
    },
    "AE": {
        "strong": {
            "AE_TERM", "AE_START_DATE_RAW", "AE_SEVERITY_RAW", "AEYN_RAW",
            "AE_OUTCOME_RAW", "AE_SER_RAW", "AE_ONGOING_RAW",
            "AE_REL_STUDY_DRUG_RAW", "VISITDT_RAW", "ENTRY_STATUS_RAW",
        },
        "weak": {
            "AE_TOXGR_RAW", "AE_START_TIME_RAW", "AE_END_DATE_RAW",
            "AE_END_TIME_RAW", "AE_FORM_SEQ", "AE_SEQ_CRF", "AE_COMMENT",
        },
    },
    "LB": {
        "strong": {
            "TEST_CODE_RAW", "TEST_NAME_RAW", "RESULT_RAW", "ORIG_UNIT_RAW",
            "SPECIMEN_RAW", "RESULT_NUM_RAW", "ABN_FLAG_RAW", "NOT_DONE_RAW",
            "LAB_SOURCE_RAW", "LB_PAGE_ID", "LB_LINE_NO",
        },
        "weak": {
            "REF_LOW_RAW", "REF_HIGH_RAW", "REF_UNIT_RAW", "COMMENT_RAW",
            "VISITDT_RAW", "COLL_DATE_RAW", "COLL_TIME_RAW",
        },
    },
    "VS": {
        "strong": {
            "VS_TEST_RAW", "VS_RESULT_RAW", "VS_UNIT_RAW", "VS_DATE",
            "VISIT_NAME", "VISIT_NUM",
        },
        "weak": {
            "POSITION_RAW", "FASTING_RAW", "SUBJECT_KEY", "VS_TIME",
            "PROTOCOL_ID", "SITE_NUMBER", "SUBJECT_NUMBER",
        },
    },
}

FILENAME_HINTS: Dict[str, str] = {
    "dm": "DM",
    "ae": "AE",
    "lb": "LB",
    "vs": "VS",
}

# Canonicalize common header variations into one shared vocabulary.
COLUMN_ALIASES: Dict[str, str] = {
    # shared formatting variants
    "VISITNUM": "VISIT_NUM",
    "VISITNUM_RAW": "VISIT_NUM",
    "VISIT_NUM_RAW": "VISIT_NUM",
    "AE_SEQ_CRF": "AE_SEQ_CRF",
    "AE_SEQ_CRF".replace("_CRF", "_CRF"): "AE_SEQ_CRF",
    "AE_SEQ_CRF".replace("_CRF", "_CRF").replace("_CRF", "_CRF"): "AE_SEQ_CRF",

    # DM CRF/raw style names
    "STUDY NUMBER": "STUDY_NUMBER",
    "STUDY_NUMBER": "STUDY_NUMBER",
    "SITE NO": "SITE_NO",
    "SITE_NO": "SITE_NO",
    "SITE NUMBER": "SITE_NUMBER",
    "SITE_NUMBER": "SITE_NUMBER",
    "SCREENING NO": "SCREENING_NO",
    "SCREENING_NO": "SCREENING_NO",
    "SUBJECT NO": "SUBJECT_NO",
    "SUBJECT_NO": "SUBJECT_NO",
    "UNIQUE SUBJECT ID": "UNIQUE_SUBJECT_ID",
    "UNIQUE_SUBJECT_ID": "UNIQUE_SUBJECT_ID",
    "DATE OF BIRTH": "DATE_OF_BIRTH",
    "DATE_OF_BIRTH": "DATE_OF_BIRTH",
    "SEX AT BIRTH": "SEX_AT_BIRTH",
    "SEX_AT_BIRTH": "SEX_AT_BIRTH",
    "RACE CATEGORY": "RACE_CATEGORY",
    "RACE_CATEGORY": "RACE_CATEGORY",
    "ETHNIC GROUP": "ETHNIC_GROUP",
    "ETHNIC_GROUP": "ETHNIC_GROUP",
    "INFORMED CONSENT DT": "INFORMED_CONSENT_DT",
    "INFORMED_CONSENT_DT": "INFORMED_CONSENT_DT",
    "FIRST DOSE DATE": "FIRST_DOSE_DATE",
    "FIRST_DOSE_DATE": "FIRST_DOSE_DATE",
    "ASSIGNED TREATMENT": "ASSIGNED_TREATMENT",
    "ASSIGNED_TREATMENT": "ASSIGNED_TREATMENT",
    "COUNTRY OF SITE": "COUNTRY_OF_SITE",
    "COUNTRY_OF_SITE": "COUNTRY_OF_SITE",
    "INVESTIGATOR NAME": "INVESTIGATOR_NAME",
    "INVESTIGATOR_NAME": "INVESTIGATOR_NAME",
    "SCREEN FAILURE FLAG": "SCREEN_FAILURE_FLAG",
    "SCREEN_FAILURE_FLAG": "SCREEN_FAILURE_FLAG",

    # direct SDTM-ish DM names
    "STUDYID": "STUDYID",
    "SITEID": "SITEID",
    "SUBJID": "SUBJID",
    "BRTHDTC": "BRTHDTC",
    "RFSTDTC": "RFSTDTC",
    "DTHFL": "DTHFL",
    "DTHDTC": "DTHDTC",
    "SEX": "SEX",
    "AGE": "AGE",
    "AGEU": "AGEU",
    "RACE": "RACE",
    "ETHNIC": "ETHNIC",
    "COUNTRY": "COUNTRY",
    "ARM": "ARM",
    "ACTARM": "ACTARM",
}


def _canon(col: str) -> str:
    c = str(col).strip().upper().replace("-", "_")
    c = " ".join(c.split())  # collapse repeated spaces first
    if c in COLUMN_ALIASES:
        return COLUMN_ALIASES[c]
    c = c.replace(" ", "_")
    return COLUMN_ALIASES.get(c, c)


def _normalize_columns(columns: Iterable[str]) -> Set[str]:
    return {_canon(col) for col in columns}


def _matched_columns_for_domain(domain: str, columns: Set[str]) -> List[str]:
    rules = DOMAIN_RULES[domain]
    return sorted((columns & rules["strong"]) | (columns & rules["weak"]))


def _filename_hint(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    stem = Path(str(name)).name.lower()
    for token, domain in FILENAME_HINTS.items():
        if f"{token}_" in stem or f"_{token}" in stem or stem.startswith(token) or f"/{token}" in stem:
            return domain
    return None


def _score_domains(columns: Set[str], file_name: Optional[str] = None) -> Tuple[Optional[str], int, List[str], Dict[str, int]]:
    scores: Dict[str, int] = {}
    strong_hit_counts: Dict[str, int] = {}
    hint_domain = _filename_hint(file_name)

    for domain, rules in DOMAIN_RULES.items():
        strong_hits = columns & rules["strong"]
        weak_hits = columns & rules["weak"]

        score = len(strong_hits) * 3 + len(weak_hits)
        if hint_domain == domain:
            score += 2

        strong_hit_counts[domain] = len(strong_hits)
        scores[domain] = score

    best_domain = max(scores, key=scores.get)
    best_score = scores[best_domain]

    # Accept filename hint fallback for demo files, but otherwise require at least one strong signal.
    if best_score == 0:
        return None, 0, [], scores

    if strong_hit_counts[best_domain] == 0 and hint_domain != best_domain:
        return None, 0, [], scores

    total_score = sum(scores.values())
    confidence = int(round((best_score / total_score) * 100)) if total_score > 0 else 0
    matched_columns = _matched_columns_for_domain(best_domain, columns)

    if not matched_columns and hint_domain == best_domain:
        matched_columns = [f"filename hint: {Path(str(file_name)).name}"]

    return best_domain, confidence, matched_columns, scores


def detect_domain_from_columns(columns: Iterable[str], file_name: Optional[str] = None) -> Dict[str, object]:
    cols = _normalize_columns(columns)
    domain, confidence, matched_columns, scores = _score_domains(cols, file_name=file_name)
    return {
        "domain": domain,
        "confidence": confidence,
        "matched_columns": matched_columns,
        "scores": scores,
    }


def detect_domain_from_dataframe(df: pd.DataFrame, file_name: Optional[str] = None) -> Dict[str, object]:
    return detect_domain_from_columns(df.columns, file_name=file_name)


def detect_domain(value, *args, **kwargs) -> Dict[str, object]:
    """
    Backward-compatible wrapper.

    Accepts:
    - detect_domain(dataframe)
    - detect_domain(columns)
    - detect_domain(dataframe, file_name)
    - detect_domain(columns, file_name)
    """
    file_name = None
    if args:
        first_extra = args[0]
        if isinstance(first_extra, (str, Path)):
            file_name = str(first_extra)

    if "file_name" in kwargs and kwargs["file_name"]:
        file_name = str(kwargs["file_name"])

    if isinstance(value, pd.DataFrame):
        return detect_domain_from_dataframe(value, file_name=file_name)
    return detect_domain_from_columns(value, file_name=file_name)
