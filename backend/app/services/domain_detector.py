from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple, Any

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
    "AETERM": "AE TERM",
    "AE_START_DATE_RAW": "AE START DATE RAW",
    "AE_SEVERITY_RAW": "AE SEVERITY RAW",
    "AEYN_RAW": "AEYN RAW",
    "AE_OUTCOME_RAW": "AE OUTCOME RAW",
    "AE_SER_RAW": "AE SER RAW",
    "AE_ONGOING_RAW": "AE ONGOING RAW",
    "AE_REL_STUDY_DRUG_RAW": "AE REL STUDY DRUG RAW",
    "VISITDT_RAW": "VISITDT RAW",
    "ENTRY_STATUS_RAW": "ENTRY STATUS RAW",
    "TEST_CODE_RAW": "TEST CODE RAW",
    "TEST_NAME_RAW": "TEST NAME RAW",
    "RESULT_RAW": "RESULT RAW",
    "ORIG_UNIT_RAW": "ORIG UNIT RAW",
    "SPECIMEN_RAW": "SPECIMEN RAW",
    "RESULT_NUM_RAW": "RESULT NUM RAW",
    "ABN_FLAG_RAW": "ABN FLAG RAW",
    "NOT_DONE_RAW": "NOT DONE RAW",
    "VS_TEST_RAW": "VS TEST RAW",
    "VS_RESULT_RAW": "VS RESULT RAW",
    "VS_UNIT_RAW": "VS UNIT RAW",
    "VS_DATE": "VS DATE",
    "VISIT_NAME": "VISIT NAME",
    "VS_TIME": "VS TIME",
    "PROTOCOL_ID": "PROTOCOL ID",
    "SITE_NUMBER": "SITE NUMBER",
    "SUBJECT_NUMBER": "SUBJECT NUMBER",
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


def _score_domains(columns: Set[str]) -> Tuple[Optional[str], List[str], Dict[str, int]]:
    scores: Dict[str, int] = {}

    for domain, rules in DOMAIN_RULES.items():
        strong_hits = columns & rules["strong"]
        weak_hits = columns & rules["weak"]
        scores[domain] = len(strong_hits) * 3 + len(weak_hits)

    best_domain = max(scores, key=scores.get)
    best_score = scores[best_domain]

    if best_score == 0:
        return None, [], scores

    matched_columns = _matched_columns_for_domain(best_domain, columns)
    return best_domain, matched_columns, scores


def _filename_hint(filename: Optional[str]) -> Tuple[Optional[str], List[str]]:
    if not filename:
        return None, []
    name = Path(filename).name.lower()
    matched = [token for token in FILENAME_HINTS if token in name]
    if len(matched) == 1:
        return FILENAME_HINTS[matched[0]], [f"filename hint: {Path(filename).name}"]
    return None, []


def _read_columns_from_file(path_like: str) -> Optional[List[str]]:
    try:
        path = Path(path_like)
        if not path.exists() or not path.is_file():
            return None

        suffix = path.suffix.lower()
        if suffix == ".csv":
            df = pd.read_csv(path, nrows=0)
            return list(df.columns)
        if suffix in {".xlsx", ".xls"}:
            df = pd.read_excel(path, nrows=0)
            return list(df.columns)
    except Exception:
        return None
    return None


def _extract_inputs(value: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Tuple[Optional[List[str]], Optional[str]]:
    filename: Optional[str] = kwargs.get("filename")
    columns: Optional[List[str]] = None

    all_values: List[Any] = [value, *args, *kwargs.values()]

    for item in all_values:
        if isinstance(item, pd.DataFrame):
            columns = list(item.columns)
            break

    if columns is None:
        for item in all_values:
            if isinstance(item, (list, tuple, set)):
                try:
                    cols = list(item)
                    if cols and all(not isinstance(x, (dict, pd.DataFrame)) for x in cols):
                        columns = [str(x) for x in cols]
                        break
                except Exception:
                    pass

    possible_strings: List[str] = []
    for item in all_values:
        if isinstance(item, (str, Path)):
            possible_strings.append(str(item))

    if filename is None and possible_strings:
        filename = possible_strings[0]

    if columns is None:
        for s in possible_strings:
            cols = _read_columns_from_file(s)
            if cols:
                columns = cols
                if filename is None:
                    filename = s
                break

    return columns, filename


def detect_domain_from_columns(columns: Iterable[object], filename: Optional[str] = None) -> Dict[str, object]:
    cols = _normalize_columns(columns)
    domain, matched_columns, _scores = _score_domains(cols)

    if domain is None:
        hinted_domain, hint_columns = _filename_hint(filename)
        if hinted_domain:
            return {
                "domain": hinted_domain,
                "matched_columns": hint_columns,
            }

    if domain is None:
        return {
            "domain": "UNKNOWN",
            "matched_columns": [],
        }

    return {
        "domain": domain,
        "matched_columns": matched_columns,
    }


def detect_domain_from_dataframe(df: pd.DataFrame, filename: Optional[str] = None) -> Dict[str, object]:
    return detect_domain_from_columns(df.columns, filename=filename)


def detect_domain(value, *args, **kwargs) -> Dict[str, object]:
    columns, filename = _extract_inputs(value, args, kwargs)

    if columns is not None:
        return detect_domain_from_columns(columns, filename=filename)

    hinted_domain, hint_columns = _filename_hint(filename)
    if hinted_domain:
        return {
            "domain": hinted_domain,
            "matched_columns": hint_columns,
        }

    return {
        "domain": "UNKNOWN",
        "matched_columns": [],
    }
