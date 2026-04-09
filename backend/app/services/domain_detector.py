from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd


# Strong/weak signal rules. Strong signals drive classification; weak signals only help break ties.
DOMAIN_RULES: Dict[str, Dict[str, Set[str]]] = {
    "DM": {
        "strong": {
            "STUDYID", "SITEID", "SUBJID", "SEX", "AGE", "AGEU", "RACE", "ETHNIC",
            "COUNTRY", "ARM", "ACTARM", "RFSTDTC", "BRTHDTC", "DTHFL", "DTHDTC",
            # Raw / CRF-style DM inputs
            "STUDY NUMBER", "SITE NO", "SITE NUMBER", "SUBJECT NO", "SCREENING NO",
            "UNIQUE SUBJECT ID", "DATE OF BIRTH", "SEX AT BIRTH", "RACE CATEGORY",
            "ETHNIC GROUP", "FIRST DOSE DATE", "ASSIGNED TREATMENT", "COUNTRY OF SITE",
            "SCREEN FAILURE FLAG",
        },
        "weak": {"SUBJECT_KEY", "USUBJID", "RAND_NO"},
    },
    "AE": {
        "strong": {
            "AE_TERM", "AE_START_DATE_RAW", "AE_SEVERITY_RAW", "AEYN_RAW",
            "AE_OUTCOME_RAW", "AE_SER_RAW", "AE_ONGOING_RAW",
            "AE_REL_STUDY_DRUG_RAW", "VISITDT_RAW", "ENTRY_STATUS_RAW",
        },
        "weak": {
            "AE_TOXGR_RAW", "AE_START_TIME_RAW", "AE_END_DATE_RAW", "AE_END_TIME_RAW",
            "AE_FORM_SEQ", "AE_SEQ_CRF", "AE_SEQ_CRF".replace("_CRF", "_CRf"), "AE_COMMENT",
        },
    },
    "LB": {
        "strong": {
            "TEST_CODE_RAW", "TEST_NAME_RAW", "RESULT_RAW", "ORIG_UNIT_RAW",
            "SPECIMEN_RAW", "RESULT_NUM_RAW", "ABN_FLAG_RAW", "NOT_DONE_RAW",
        },
        "weak": {
            "REF_LOW_RAW", "REF_HIGH_RAW", "REF_UNIT_RAW", "COMMENT_RAW",
            "VISITDT_RAW", "COLL_DATE_RAW", "COLL_TIME_RAW",
        },
    },
    "VS": {
        "strong": {
            "VS_TEST_RAW", "VS_RESULT_RAW", "VS_UNIT_RAW", "VS_DATE", "VISIT_NAME", "VISIT_NUM",
        },
        "weak": {
            "POSITION_RAW", "FASTING_RAW", "SUBJECT_KEY", "VS_TIME", "PROTOCOL_ID",
            "SITE_NUMBER", "SUBJECT_NUMBER",
        },
    },
}


COLUMN_ALIASES: Dict[str, str] = {
    "VISITNUM": "VISIT_NUM",
    "VISITNUM_RAW": "VISIT_NUM",
    "VISIT_NUM_RAW": "VISIT_NUM",
    "AE_SEQ_CRF": "AE_SEQ_CRF",
    "AE_SEQ_CRF".replace("_CRF", "_CRf"): "AE_SEQ_CRF",
    # common CRF label / machine-name harmonization for DM
    "STUDY_NUMBER": "STUDY NUMBER",
    "SITE_NO": "SITE NO",
    "SITE_NUMBER": "SITE NUMBER",
    "SUBJECT_NO": "SUBJECT NO",
    "UNIQUE_SUBJECT_ID": "UNIQUE SUBJECT ID",
    "DATE_OF_BIRTH": "DATE OF BIRTH",
    "SEX_AT_BIRTH": "SEX AT BIRTH",
    "RACE_CATEGORY": "RACE CATEGORY",
    "ETHNIC_GROUP": "ETHNIC GROUP",
    "FIRST_DOSE_DATE": "FIRST DOSE DATE",
    "ASSIGNED_TREATMENT": "ASSIGNED TREATMENT",
    "COUNTRY_OF_SITE": "COUNTRY OF SITE",
    "SCREEN_FAILURE_FLAG": "SCREEN FAILURE FLAG",
}


FILENAME_HINTS: Dict[str, str] = {
    "dm": "DM",
    "ae": "AE",
    "lb": "LB",
    "vs": "VS",
}


def _normalize_column_name(col: object) -> str:
    c = str(col).strip().upper().replace("-", " ")
    c = " ".join(c.split())
    return COLUMN_ALIASES.get(c, c)


def _normalize_columns(columns: Iterable[object]) -> Set[str]:
    return {_normalize_column_name(col) for col in columns}


def _matched_columns_for_domain(domain: str, columns: Set[str]) -> List[str]:
    rules = DOMAIN_RULES[domain]
    return sorted((columns & rules["strong"]) | (columns & rules["weak"]))


def _score_domains(columns: Set[str]) -> Tuple[Optional[str], float, List[str], Dict[str, int]]:
    scores: Dict[str, int] = {}
    strong_hit_counts: Dict[str, int] = {}

    for domain, rules in DOMAIN_RULES.items():
        strong_hits = columns & rules["strong"]
        weak_hits = columns & rules["weak"]
        strong_hit_counts[domain] = len(strong_hits)
        # Strong hits dominate; weak hits are tie-breakers only.
        scores[domain] = len(strong_hits) * 10 + len(weak_hits)

    best_domain = max(scores, key=scores.get) if scores else None
    if not best_domain:
        return None, 0.0, [], scores

    best_score = scores[best_domain]
    best_strong_hits = strong_hit_counts[best_domain]
    matched_columns = _matched_columns_for_domain(best_domain, columns)

    # Need at least one strong signal to auto-classify from columns.
    if best_score == 0 or best_strong_hits == 0:
        return None, 0.0, [], scores

    # Confidence is normalized to 0-100 based on the winning domain's own available signals.
    max_score_for_domain = len(DOMAIN_RULES[best_domain]["strong"]) * 10 + len(DOMAIN_RULES[best_domain]["weak"])
    confidence = (best_score / max_score_for_domain) * 1.0 if max_score_for_domain else 0.0
    confidence = round(max(0.0, min(confidence, 100.0)), 1)

    return best_domain, confidence, matched_columns, scores


def _domain_from_filename_hint(filename: Optional[str]) -> Tuple[Optional[str], float, List[str]]:
    if not filename:
        return None, 0.0, []

    name = Path(str(filename)).name.lower()
    for token, domain in FILENAME_HINTS.items():
        if token in name:
            # Keep filename-hint confidence high but bounded.
            return domain, 95.0, [f"filename hint: {Path(str(filename)).name}"]
    return None, 0.0, []


def _extract_columns_and_filename(value, args: Sequence[object]) -> Tuple[Optional[Set[str]], Optional[str]]:
    filename: Optional[str] = None
    columns: Optional[Set[str]] = None

    if isinstance(value, pd.DataFrame):
        columns = _normalize_columns(value.columns)
    elif isinstance(value, (list, tuple, set)):
        columns = _normalize_columns(value)
    elif isinstance(value, str):
        # Could be a filename or a single column, but for this backend it's usually a filename hint.
        filename = value
    elif value is not None:
        try:
            columns = _normalize_columns(value)
        except TypeError:
            filename = str(value)

    for arg in args:
        if isinstance(arg, pd.DataFrame):
            columns = _normalize_columns(arg.columns)
        elif isinstance(arg, (list, tuple, set)):
            columns = _normalize_columns(arg)
        elif isinstance(arg, str) and filename is None:
            filename = arg

    return columns, filename


def detect_domain_from_columns(columns: Iterable[object], filename: Optional[str] = None) -> Dict[str, object]:
    cols = _normalize_columns(columns)
    domain, confidence, matched_columns, scores = _score_domains(cols)

    if domain is None:
        hint_domain, hint_confidence, hint_matches = _domain_from_filename_hint(filename)
        if hint_domain is not None:
            return {
                "domain": hint_domain,
                "confidence": hint_confidence,
                "matched_columns": hint_matches,
                "scores": scores,
            }

    return {
        "domain": domain,
        "confidence": confidence,
        "matched_columns": matched_columns,
        "scores": scores,
    }


def detect_domain_from_dataframe(df: pd.DataFrame, filename: Optional[str] = None) -> Dict[str, object]:
    return detect_domain_from_columns(df.columns, filename=filename)


def detect_domain(value=None, *args, **kwargs) -> Dict[str, object]:
    """
    Backward-compatible wrapper.

    Supports calls like:
    - detect_domain(df)
    - detect_domain(columns)
    - detect_domain(df, filename)
    - detect_domain(filename, df)
    - detect_domain(value, *extra_unused_args)
    """
    filename = kwargs.get("filename")
    columns, inferred_filename = _extract_columns_and_filename(value, args)
    filename = filename or inferred_filename

    if columns is not None:
        return detect_domain_from_columns(columns, filename=filename)

    hint_domain, hint_confidence, hint_matches = _domain_from_filename_hint(filename)
    return {
        "domain": hint_domain,
        "confidence": hint_confidence if hint_domain else 0.0,
        "matched_columns": hint_matches,
        "scores": {domain: 0 for domain in DOMAIN_RULES},
    }
