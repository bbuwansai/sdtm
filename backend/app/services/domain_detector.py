from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd


DOMAIN_RULES: Dict[str, Dict[str, Set[str]]] = {
    "DM": {
        "strong": {
            "STUDYID", "SITEID", "SUBJID", "SEX", "AGE", "AGEU", "RACE", "ETHNIC",
            "COUNTRY", "ARM", "ACTARM", "RFSTDTC", "BRTHDTC", "DTHFL", "DTHDTC",
            "SITE NO", "SUBJECT NO", "SCREENING NO", "RAND NO",
            "STUDY NUMBER", "UNIQUE SUBJECT ID", "DATE OF BIRTH", "SEX AT BIRTH",
            "RACE CATEGORY", "ETHNIC GROUP", "FIRST DOSE DATE", "ASSIGNED TREATMENT",
            "COUNTRY OF SITE", "SCREEN FAILURE FLAG",
        },
        "weak": {"SUBJECT KEY", "USUBJID"},
    },
    "AE": {
        "strong": {
            "AE TERM", "AE START DATE RAW", "AE SEVERITY RAW", "AEYN RAW",
            "AE OUTCOME RAW", "AE SER RAW", "AE ONGOING RAW",
            "AE REL STUDY DRUG RAW", "VISITDT RAW", "ENTRY STATUS RAW",
        },
        "weak": {
            "AE TOXGR RAW", "AE START TIME RAW", "AE END DATE RAW",
            "AE END TIME RAW", "AE FORM SEQ", "AE SEQ CRF", "AE COMMENT",
        },
    },
    "LB": {
        "strong": {
            "TEST CODE RAW", "TEST NAME RAW", "RESULT RAW", "ORIG UNIT RAW",
            "SPECIMEN RAW", "RESULT NUM RAW", "ABN FLAG RAW", "NOT DONE RAW",
        },
        "weak": {
            "REF LOW RAW", "REF HIGH RAW", "REF UNIT RAW", "COMMENT RAW",
            "VISITDT RAW", "COLL DATE RAW", "COLL TIME RAW",
        },
    },
    "VS": {
        "strong": {
            "VS TEST RAW", "VS RESULT RAW", "VS UNIT RAW", "VS DATE",
            "VISIT NAME", "VISIT NUM",
        },
        "weak": {
            "POSITION RAW", "FASTING RAW", "SUBJECT KEY", "VS TIME",
            "PROTOCOL ID", "SITE NUMBER", "SUBJECT NUMBER",
        },
    },
}

COLUMN_ALIASES: Dict[str, str] = {
    "VISITNUM": "VISIT NUM",
    "VISITNUMRAW": "VISIT NUM",
    "VISIT_NUM_RAW": "VISIT NUM",
    "VISIT_NUM": "VISIT NUM",
    "AESEQCRF": "AE SEQ CRF",
    "AE_SEQ_CRF": "AE SEQ CRF",
    "AE_SEQ_CRf": "AE SEQ CRF",
    "SUBJECTKEY": "SUBJECT KEY",
    "STUDYNUMBER": "STUDY NUMBER",
    "SITENO": "SITE NO",
    "SUBJECTNO": "SUBJECT NO",
    "SCREENINGNO": "SCREENING NO",
    "RANDNO": "RAND NO",
    "UNIQUESUBJECTID": "UNIQUE SUBJECT ID",
    "DATEOFBIRTH": "DATE OF BIRTH",
    "SEXATBIRTH": "SEX AT BIRTH",
    "RACECATEGORY": "RACE CATEGORY",
    "ETHNICGROUP": "ETHNIC GROUP",
    "FIRSTDOSEDATE": "FIRST DOSE DATE",
    "ASSIGNEDTREATMENT": "ASSIGNED TREATMENT",
    "COUNTRYOFSITE": "COUNTRY OF SITE",
    "SCREENFAILUREFLAG": "SCREEN FAILURE FLAG",
}

FILENAME_HINTS: Dict[str, str] = {
    "dm": "DM",
    "ae": "AE",
    "lb": "LB",
    "vs": "VS",
}


def _normalize_column_name(col: object) -> str:
    c = str(col).strip().upper().replace("-", " ").replace("_", " ")
    c = " ".join(c.split())
    compact = c.replace(" ", "")
    return COLUMN_ALIASES.get(compact, COLUMN_ALIASES.get(c, c))


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
        scores[domain] = len(strong_hits) * 3 + len(weak_hits)

    best_domain = max(scores, key=scores.get)
    best_score = scores[best_domain]

    if best_score == 0 or strong_hit_counts[best_domain] == 0:
        return None, 0.0, [], scores

    total_score = sum(scores.values())
    confidence = (best_score / total_score) if total_score > 0 else 0.0
    confidence = max(0.0, min(confidence, 1.0))
    matched_columns = _matched_columns_for_domain(best_domain, columns)
    return best_domain, confidence, matched_columns, scores


def _filename_hint(filename: Optional[str]) -> Tuple[Optional[str], List[str]]:
    if not filename:
        return None, []
    name = Path(filename).name.lower()
    matched = [token for token in FILENAME_HINTS if token in name]
    if len(matched) == 1:
        return FILENAME_HINTS[matched[0]], [f"filename hint: {Path(filename).name}"]
    return None, []


def detect_domain_from_columns(columns: Iterable[object], filename: Optional[str] = None) -> Dict[str, object]:
    cols = _normalize_columns(columns)
    domain, confidence, matched_columns, scores = _score_domains(cols)

    if domain is None:
        hinted_domain, hint_columns = _filename_hint(filename)
        if hinted_domain is not None:
            domain = hinted_domain
            confidence = 0.95
            matched_columns = hint_columns

    return {
        "domain": domain,
        "confidence": round(float(confidence), 4),  # 0.0 to 1.0 for frontend display
        "matched_columns": matched_columns,
        "scores": scores,
    }


def detect_domain_from_dataframe(df: pd.DataFrame, filename: Optional[str] = None) -> Dict[str, object]:
    return detect_domain_from_columns(df.columns, filename=filename)


def detect_domain(value, *args, **kwargs) -> Dict[str, object]:
    filename = kwargs.get("filename")

    if isinstance(value, pd.DataFrame):
        if args and isinstance(args[0], str):
            filename = args[0]
        return detect_domain_from_dataframe(value, filename=filename)

    if isinstance(value, str):
        if args and isinstance(args[0], pd.DataFrame):
            return detect_domain_from_dataframe(args[0], filename=value)

        hinted_domain, hint_columns = _filename_hint(value)
        return {
            "domain": hinted_domain,
            "confidence": 0.95 if hinted_domain else 0.0,
            "matched_columns": hint_columns,
            "scores": {},
        }

    return detect_domain_from_columns(value, filename=filename)
