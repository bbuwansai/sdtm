import json
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Set
import traceback

import pandas as pd

BASE = Path(__file__).resolve().parent
OUTPUT_DIR = BASE / "sdtm_outputs_v4"

CLEAN_SOURCE_CANDIDATES = [
    "dm_cleaned_output.csv",
    "dm_cleaned_generated_by_code.csv",
    "dm_issue_detected_clean.csv",
    "layer1_cleaned_dm.csv",
]

RAW_SOURCE_CANDIDATES = [
    "dm_raw.csv",
    "dm_raw_demo_50_rows.csv",
    "dm_raw_crf_style_50_rows.csv",
    "dm_source_50_rows.csv",
]

CONTROLLED_TERMS_CANDIDATES = ["controlled_terminology_demo.csv"]
STUDY_META_CANDIDATES = ["study_metadata_demo.json"]
PROGRAMMING_CONVENTIONS_CANDIDATES = ["programming_conventions_demo.json"]
SPONSOR_RULES_CANDIDATES = ["demo_sponsor_rules_dm.json"]

LAYER1_HUMAN_CANDIDATES = [
    "dm_human_review_issues.csv",
    "dm_issue_log_human.csv",
    "dm_issue_log_human_v2.csv",
    "dm_issue_log_human_v3.csv",
]

LAYER1_SDTM_FIXABLE_CANDIDATES = [
    "dm_sdtm_standardizable_issues.csv",
    "dm_issue_log_sdtm_standardisable.csv",
    "dm_issue_log_sdtm_standardizable.csv",
    "dm_issue_log_sdtm_standardisable_v2.csv",
    "dm_issue_log_sdtm_standardizable_v2.csv",
]

SPEC_CANDIDATES = [
    "dm_mapping_spec.csv",
    "dm_mapping_spec_validated.csv",
    "dm_mapping_spec_validated_v2.csv",
    "dm_spec.csv",
    "dm_spec_validated.csv",
]

FINAL_COLUMNS = [
    "STUDYID", "DOMAIN", "USUBJID", "SUBJID", "SITEID",
    "RFSTDTC", "BRTHDTC", "AGE", "AGEU", "SEX",
    "RACE", "ETHNIC", "COUNTRY", "ARM", "ACTARM",
    "DTHFL", "DTHDTC",
]

EXCEPTION_COLUMNS = FINAL_COLUMNS + ["SOURCE_ROW_NUMBER", "ROW_ISSUES"]
AUTO_LOG_COLUMNS = ["SOURCE_ROW_NUMBER", "AUTO_ACTIONS"]
BUILD_SUMMARY_COLUMNS = ["metric", "value"]


def first_existing(base: Path, candidates: List[str], required: bool = True) -> Optional[Path]:
    for c in candidates:
        p = base / c
        if p.exists():
            return p
    if required:
        raise FileNotFoundError(f"None of these files were found: {candidates}")
    return None


def find_best_file(project_folder: Path, candidates: List[str], required: bool = True) -> Optional[Path]:
    matches: List[Path] = []
    lowered = [c.lower() for c in candidates]
    for p in project_folder.rglob("*"):
        if not p.is_file():
            continue
        name = p.name.lower()
        if any(c == name or c in name for c in lowered):
            matches.append(p)

    if not matches:
        if required:
            raise FileNotFoundError(f"Could not auto-detect any of these files under {project_folder}: {candidates}")
        return None

    def score(path: Path) -> Tuple[int, int, str]:
        name = path.name.lower()
        exact_score = 0
        contains_score = 0
        for i, c in enumerate(lowered):
            if name == c:
                exact_score = max(exact_score, 1000 - i)
            elif c in name:
                contains_score = max(contains_score, 200 - i)
        return (exact_score, contains_score, str(path))

    matches = sorted(matches, key=score, reverse=True)
    return matches[0]


def auto_detect_inputs(project_folder: Path) -> Dict[str, Optional[Path]]:
    clean_source_path = first_existing(project_folder, CLEAN_SOURCE_CANDIDATES, required=False) or find_best_file(project_folder, CLEAN_SOURCE_CANDIDATES, required=True)
    raw_source_path = first_existing(project_folder, RAW_SOURCE_CANDIDATES, required=False) or find_best_file(project_folder, RAW_SOURCE_CANDIDATES, required=False)
    ct_path = first_existing(project_folder, CONTROLLED_TERMS_CANDIDATES, required=False) or find_best_file(project_folder, CONTROLLED_TERMS_CANDIDATES, required=True)
    meta_path = first_existing(project_folder, STUDY_META_CANDIDATES, required=False) or find_best_file(project_folder, STUDY_META_CANDIDATES, required=True)
    prog_path = first_existing(project_folder, PROGRAMMING_CONVENTIONS_CANDIDATES, required=False) or find_best_file(project_folder, PROGRAMMING_CONVENTIONS_CANDIDATES, required=True)
    sponsor_rules_path = first_existing(project_folder, SPONSOR_RULES_CANDIDATES, required=False) or find_best_file(project_folder, SPONSOR_RULES_CANDIDATES, required=True)
    human_path = first_existing(project_folder, LAYER1_HUMAN_CANDIDATES, required=False) or find_best_file(project_folder, LAYER1_HUMAN_CANDIDATES, required=False)
    sdtm_fixable_path = first_existing(project_folder, LAYER1_SDTM_FIXABLE_CANDIDATES, required=False) or find_best_file(project_folder, LAYER1_SDTM_FIXABLE_CANDIDATES, required=False)
    spec_path = first_existing(project_folder, SPEC_CANDIDATES, required=False) or find_best_file(project_folder, SPEC_CANDIDATES, required=False)

    return {
        "clean_source": clean_source_path,
        "raw_source": raw_source_path,
        "ct": ct_path,
        "meta": meta_path,
        "prog": prog_path,
        "sponsor_rules": sponsor_rules_path,
        "human_issues": human_path,
        "sdtm_fixable_issues": sdtm_fixable_path,
        "spec": spec_path,
    }


def load_inputs(project_folder: Path):
    used = auto_detect_inputs(project_folder)

    source_df = pd.read_csv(used["clean_source"], dtype=str)
    ct_df = pd.read_csv(used["ct"], dtype=str)
    study_meta = json.loads(used["meta"].read_text(encoding="utf-8"))
    prog_conv = json.loads(used["prog"].read_text(encoding="utf-8"))
    sponsor_rules = json.loads(used["sponsor_rules"].read_text(encoding="utf-8"))

    raw_df = pd.read_csv(used["raw_source"], dtype=str) if used["raw_source"] else pd.DataFrame()
    human_df = pd.read_csv(used["human_issues"], dtype=str) if used["human_issues"] else pd.DataFrame()
    sdtm_fixable_df = pd.read_csv(used["sdtm_fixable_issues"], dtype=str) if used["sdtm_fixable_issues"] else pd.DataFrame()
    spec_df = pd.read_csv(used["spec"], dtype=str) if used["spec"] else pd.DataFrame()

    return source_df, raw_df, ct_df, study_meta, prog_conv, sponsor_rules, human_df, sdtm_fixable_df, spec_df, used


def clean_value(val: Optional[str], missing_values: List[str]) -> Optional[str]:
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "":
        return None
    if s.upper() in {str(x).upper() for x in missing_values}:
        return None
    if s.upper() in {"NAN", "NONE"}:
        return None
    return s


def normalize_df(df: pd.DataFrame, prog_conv: dict) -> pd.DataFrame:
    out = df.copy()
    missing_values = prog_conv.get("missing_values", ["", "NA", "NULL"])
    for col in out.columns:
        out[col] = out[col].apply(lambda x: clean_value(x, missing_values))
    return out


def normalize_id(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def normalize_age_token(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    import re
    s = str(val).strip()
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)", s)
    if not m:
        return None
    try:
        f = float(m.group(1))
        return str(int(f)) if f.is_integer() else str(f)
    except Exception:
        return None


def to_iso_partial_date(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    import re
    s = str(val).strip()
    if re.fullmatch(r"\d{4}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
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


def is_full_date(val: Optional[str]) -> bool:
    return isinstance(val, str) and len(val) == 10


def calculate_age_years(brthdtc: str, rfstdtc: str) -> Optional[str]:
    try:
        b = pd.to_datetime(brthdtc)
        r = pd.to_datetime(rfstdtc)
        years = r.year - b.year - ((r.month, r.day) < (b.month, b.day))
        return str(years)
    except Exception:
        return None


def make_ct_lookup(ct_df):
    lut = {}

    cols = {str(c).strip().lower(): c for c in ct_df.columns}

    var_col = (
        cols.get("variable")
        or cols.get("var")
        or cols.get("column")
        or cols.get("field")
        or cols.get("codelist_name")
    )

    src_col = (
        cols.get("source_value")
        or cols.get("source")
        or cols.get("raw_value")
        or cols.get("value")
        or cols.get("code")
        or cols.get("term")
    )

    std_col = (
        cols.get("standard_value")
        or cols.get("standard")
        or cols.get("mapped_value")
        or cols.get("target_value")
        or cols.get("normalized_value")
        or cols.get("value")
    )

    if var_col is None or src_col is None or std_col is None:
        raise ValueError(f"CT file missing required columns. Found: {list(ct_df.columns)}")

    for _, row in ct_df.iterrows():
        var = str(row[var_col]).strip().upper()
        src = "" if pd.isna(row[src_col]) else str(row[src_col]).strip().upper()
        std = "" if pd.isna(row[std_col]) else str(row[std_col]).strip().upper()

        if not var:
            continue

        if src:
            lut[(var, src)] = std if std else src

    return lut


def apply_ct(variable: str, value: Optional[str], lut: Dict[tuple, str]) -> Optional[str]:
    if value is None:
        return None
    key = (variable.upper(), str(value).strip().upper())
    return lut.get(key, str(value).strip().upper())


def standardize_ageu(value: Optional[str], age_present: bool, sponsor_rules: dict, lut: Dict[tuple, str]) -> Optional[str]:
    if value is not None:
        mapped = apply_ct("AGEU", value, lut)
        if mapped in {"YEAR", "YEARS", "YR", "YRS"}:
            return "YEARS"
        return mapped
    default = sponsor_rules.get("ageu_rule", {}).get("default_if_age_present")
    if age_present and default:
        return default
    return None


def standardize_country(value: Optional[str], sponsor_rules: dict, lut: Dict[tuple, str]) -> Optional[str]:
    if value is None:
        return None
    v = apply_ct("COUNTRY", value, lut)
    if len(v) == 3:
        return v
    if sponsor_rules.get("country_rule", {}).get("pass_through_if_already_iso3") and len(str(value).strip()) == 3:
        return str(value).strip().upper()
    return v


def standardize_dthfl(dthfl_val: Optional[str], dthdtc_val: Optional[str], sponsor_rules: dict, lut: Dict[tuple, str]) -> Optional[str]:
    if dthdtc_val and sponsor_rules.get("dthfl_rule", {}).get("set_Y_if_dthdtc_present", True):
        return "Y"
    if dthfl_val is None:
        return None
    mapped = apply_ct("DTHFL", dthfl_val, lut)
    if mapped in {"Y", "YES"}:
        return "Y"
    if sponsor_rules.get("dthfl_rule", {}).get("allow_only_Y_or_blank", True):
        return None
    return mapped


def derive_usubjid(row: dict, sponsor_rules: dict) -> Optional[str]:
    rule = sponsor_rules.get("usubjid_rule", "STUDYID-SITEID-SUBJID").upper()
    if row.get("USUBJID"):
        return row.get("USUBJID")
    if rule == "STUDYID-SITEID-SUBJID":
        if row.get("STUDYID") and row.get("SITEID") and row.get("SUBJID"):
            return f"{row['STUDYID']}-{row['SITEID']}-{row['SUBJID']}"
        return None
    if rule == "SUBJECT_KEY":
        return row.get("SUBJECT_KEY")
    return None


def get_row_value(row: pd.Series, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c not in row:
            continue
        val = row.get(c)
        if pd.isna(val):
            continue
        sval = str(val).strip()
        if sval == "" or sval.upper() in {"NAN", "NONE", "NULL"}:
            continue
        return sval
    return None


def summarize_issue_log(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["source_row_number", "issue_count", "rule_ids", "messages", "fields"])

    tmp = df.copy()

    row_col = None
    for c in ["source_row_number", "row_num", "SOURCE_ROW_NUMBER", "ROW_NUM"]:
        if c in tmp.columns:
            row_col = c
            break
    if row_col is None:
        return pd.DataFrame(columns=["source_row_number", "issue_count", "rule_ids", "messages", "fields"])

    tmp["source_row_number"] = pd.to_numeric(tmp[row_col], errors="coerce")
    tmp = tmp.dropna(subset=["source_row_number"]).copy()
    tmp["source_row_number"] = tmp["source_row_number"].astype(int)

    rule_col = next((c for c in ["rule_id", "RULE_ID"] if c in tmp.columns), None)
    msg_col = next((c for c in ["message", "rule_description", "RULE_DESCRIPTION"] if c in tmp.columns), None)
    field_col = next((c for c in ["field", "FIELD"] if c in tmp.columns), None)

    if rule_col is None:
        tmp["__rule_id__"] = ""
        rule_col = "__rule_id__"
    if msg_col is None:
        tmp["__message__"] = ""
        msg_col = "__message__"
    if field_col is None:
        tmp["__field__"] = ""
        field_col = "__field__"

    return (
        tmp.groupby("source_row_number", dropna=False)
           .agg(
               issue_count=(rule_col, "count"),
               rule_ids=(rule_col, lambda s: " | ".join(sorted(pd.Series(s).dropna().astype(str).unique()))),
               messages=(msg_col, lambda s: " || ".join(pd.Series(s).dropna().astype(str).unique())),
               fields=(field_col, lambda s: " | ".join(sorted([x for x in pd.Series(s).dropna().astype(str).unique() if x]))),
           )
           .reset_index()
    )


def rows_with_human_issues(human_df: pd.DataFrame) -> set:
    if human_df.empty:
        return set()
    agg = summarize_issue_log(human_df)
    return set(agg["source_row_number"].tolist()) if not agg.empty else set()


def rows_with_sdtm_fixable_issues(sdtm_fixable_df: pd.DataFrame) -> set:
    if sdtm_fixable_df.empty:
        return set()
    agg = summarize_issue_log(sdtm_fixable_df)
    return set(agg["source_row_number"].tolist()) if not agg.empty else set()


def spec_target_order(spec_df: pd.DataFrame) -> List[str]:
    if spec_df.empty:
        return FINAL_COLUMNS.copy()
    for c in ["target_variable", "TARGET_VARIABLE", "variable", "VARIABLE"]:
        if c in spec_df.columns:
            ordered = [str(x).strip() for x in spec_df[c].dropna().tolist() if str(x).strip()]
            ordered = [x for x in ordered if x in FINAL_COLUMNS]
            if ordered:
                seen = set()
                out = []
                for v in ordered + FINAL_COLUMNS:
                    if v not in seen and v in FINAL_COLUMNS:
                        seen.add(v)
                        out.append(v)
                return out
    return FINAL_COLUMNS.copy()


def make_row_reference_df(source_df: pd.DataFrame) -> pd.DataFrame:
    out = source_df.copy()
    if "L1_SOURCE_ROW_NUMBER" not in out.columns:
        out.insert(0, "L1_SOURCE_ROW_NUMBER", range(1, len(out) + 1))
    out["L1_SOURCE_ROW_NUMBER"] = pd.to_numeric(out["L1_SOURCE_ROW_NUMBER"], errors="coerce")
    out["L1_SOURCE_ROW_NUMBER"] = out["L1_SOURCE_ROW_NUMBER"].fillna(pd.Series(range(1, len(out) + 1), index=out.index))
    out["L1_SOURCE_ROW_NUMBER"] = out["L1_SOURCE_ROW_NUMBER"].astype(int)
    return out


def apply_human_issue_gate(source_df: pd.DataFrame, human_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    ref_df = make_row_reference_df(source_df)
    human_rows = rows_with_human_issues(human_df)
    candidate_df = ref_df[~ref_df["L1_SOURCE_ROW_NUMBER"].isin(human_rows)].copy()
    human_rejected = ref_df[ref_df["L1_SOURCE_ROW_NUMBER"].isin(human_rows)].copy()
    return candidate_df, human_rejected


def apply_sdtm_fixable_gate(source_df: pd.DataFrame, sdtm_fixable_df: pd.DataFrame) -> Set[int]:
    return rows_with_sdtm_fixable_issues(sdtm_fixable_df)


def has_field_issue(rownum: int, field_name: str, issue_log_df: pd.DataFrame) -> bool:
    if issue_log_df.empty:
        return False

    row_cols = [c for c in ["source_row_number", "row_num", "SOURCE_ROW_NUMBER", "ROW_NUM"] if c in issue_log_df.columns]
    field_cols = [c for c in ["field", "FIELD"] if c in issue_log_df.columns]
    if not row_cols or not field_cols:
        return False

    row_col = row_cols[0]
    field_col = field_cols[0]

    tmp = issue_log_df.copy()
    tmp["__rownum__"] = pd.to_numeric(tmp[row_col], errors="coerce")
    tmp = tmp.dropna(subset=["__rownum__"])
    tmp["__rownum__"] = tmp["__rownum__"].astype(int)
    tmp["__field__"] = tmp[field_col].astype(str).str.upper()

    return ((tmp["__rownum__"] == int(rownum)) & (tmp["__field__"] == str(field_name).upper())).any()


def should_apply_sdtm_fix(rownum: int, field_name: str, sdtm_fixable_df: pd.DataFrame, sdtm_fixable_rows: Set[int]) -> bool:
    if rownum not in sdtm_fixable_rows:
        return False
    if sdtm_fixable_df.empty:
        return True
    return has_field_issue(rownum, field_name, sdtm_fixable_df)


def has_human_field_issue(rownum: int, field_name: str, human_df: pd.DataFrame) -> bool:
    return has_field_issue(rownum, field_name, human_df)


def build_dm_row(
    src_row: pd.Series,
    rownum: int,
    ct_lut: Dict[tuple, str],
    study_meta: dict,
    prog_conv: dict,
    sponsor_rules: dict,
    sdtm_fixable_df: pd.DataFrame,
    sdtm_fixable_rows: Set[int],
) -> Tuple[Dict[str, Optional[str]], List[str], List[str]]:
    row_issues: List[str] = []
    auto_actions: List[str] = []

    out = {c: get_row_value(src_row, c) for c in FINAL_COLUMNS}
    original = out.copy()

    out["STUDYID"] = get_row_value(src_row, "STUDYID", "PROTOCOL_ID") or study_meta.get("study_id")
    out["DOMAIN"] = "DM"
    out["SUBJID"] = normalize_id(get_row_value(src_row, "SUBJID", "SUBJECT_NUMBER"))
    out["SITEID"] = normalize_id(get_row_value(src_row, "SITEID", "SITE_NUMBER"))

    out["RFSTDTC"] = to_iso_partial_date(get_row_value(src_row, "RFSTDTC", "REF_START_DT"))
    out["BRTHDTC"] = to_iso_partial_date(get_row_value(src_row, "BRTHDTC", "DATE_OF_BIRTH"))
    out["DTHDTC"] = to_iso_partial_date(get_row_value(src_row, "DTHDTC", "DEATH_DATE"))

    collected_age = normalize_age_token(get_row_value(src_row, "AGE", "AGE_AT_REF"))
    can_fix_age = should_apply_sdtm_fix(rownum, "AGE", sdtm_fixable_df, sdtm_fixable_rows)
    if (
        can_fix_age
        and sponsor_rules.get("age_rule", {}).get("derive_if_full_dates_available", True)
        and is_full_date(out["BRTHDTC"])
        and is_full_date(out["RFSTDTC"])
    ):
        derived_age = calculate_age_years(out["BRTHDTC"], out["RFSTDTC"])
        if derived_age is not None:
            out["AGE"] = derived_age
            if derived_age != collected_age:
                auto_actions.append("Derived AGE from BRTHDTC and RFSTDTC")
        else:
            out["AGE"] = collected_age
            if collected_age is None:
                row_issues.append("AGE unavailable: could not derive and no collected AGE")
    else:
        out["AGE"] = collected_age

    can_fix_ageu = should_apply_sdtm_fix(rownum, "AGEU", sdtm_fixable_df, sdtm_fixable_rows)
    if can_fix_ageu:
        new_ageu = standardize_ageu(get_row_value(src_row, "AGEU", "AGE_UNITS"), out["AGE"] is not None, sponsor_rules, ct_lut)
        if new_ageu != get_row_value(src_row, "AGEU", "AGE_UNITS") and new_ageu is not None:
            auto_actions.append("Standardized or defaulted AGEU")
        out["AGEU"] = new_ageu
    else:
        out["AGEU"] = get_row_value(src_row, "AGEU", "AGE_UNITS")

    out["SEX"] = get_row_value(src_row, "SEX", "SEX_AT_BIRTH")
    out["RACE"] = apply_ct("RACE", get_row_value(src_row, "RACE", "RACE_CAT"), ct_lut) if get_row_value(src_row, "RACE", "RACE_CAT") is not None else None
    out["ETHNIC"] = apply_ct("ETHNIC", get_row_value(src_row, "ETHNIC", "ETHNIC_GRP"), ct_lut) if get_row_value(src_row, "ETHNIC", "ETHNIC_GRP") is not None else None
    out["COUNTRY"] = get_row_value(src_row, "COUNTRY", "COUNTRY_CODE")

    arm = get_row_value(src_row, "ARM", "PLANNED_TRT_ARM")
    actarm = get_row_value(src_row, "ACTARM", "ACTUAL_TRT_ARM")
    out["ARM"] = arm.upper() if isinstance(arm, str) and prog_conv.get("uppercase_char_fields", False) else arm
    out["ACTARM"] = actarm.upper() if isinstance(actarm, str) and prog_conv.get("uppercase_char_fields", False) else actarm

    out["DTHFL"] = get_row_value(src_row, "DTHFL", "DEATH_IND")

    can_fix_usubjid = should_apply_sdtm_fix(rownum, "USUBJID", sdtm_fixable_df, sdtm_fixable_rows)
    helper_row = {**{k: src_row.get(k) for k in src_row.index}, **out}
    if can_fix_usubjid:
        derived_usubjid = derive_usubjid(helper_row, sponsor_rules)
        out["USUBJID"] = derived_usubjid
        if derived_usubjid and derived_usubjid != original.get("USUBJID"):
            auto_actions.append("Derived USUBJID from STUDYID/SITEID/SUBJID")
    else:
        out["USUBJID"] = get_row_value(src_row, "USUBJID")

    return out, row_issues, auto_actions


def validate_dm_row(
    out: Dict[str, Optional[str]],
    original: Dict[str, Optional[str]],
    rownum: int,
    sponsor_rules: dict,
    sdtm_fixable_df: pd.DataFrame,
    human_df: pd.DataFrame,
) -> Tuple[List[str], bool]:
    row_issues: List[str] = []
    hard_fail = False

    subjid_pattern = sponsor_rules.get("subjid_rule", {}).get("regex")
    if subjid_pattern:
        import re
        subjid_re = re.compile(subjid_pattern)
    else:
        subjid_re = None

    required_for_final = set(sponsor_rules.get("required_for_demo_final", ["STUDYID","DOMAIN","USUBJID","SUBJID","SITEID","SEX"]))
    required_for_final.update({"ARM", "ACTARM"})

    for req in required_for_final:
        if not out.get(req):
            row_issues.append(f"{req} missing")
            hard_fail = True

    if out.get("SUBJID") and subjid_re and not subjid_re.fullmatch(str(out["SUBJID"])):
        row_issues.append("SUBJID violates sponsor pattern")
        hard_fail = True

    if out.get("AGE") is not None:
        try:
            age_num = float(out["AGE"])
            if age_num < 0:
                row_issues.append("AGE < 0")
                hard_fail = True
            if age_num < 18:
                row_issues.append("AGE below adult threshold")
                hard_fail = True
            if age_num > 120:
                row_issues.append("AGE implausibly high")
                hard_fail = True
        except Exception:
            row_issues.append("AGE not numeric")
            hard_fail = True

    if out.get("AGE") is not None and sponsor_rules.get("ageu_rule", {}).get("default_if_age_present") == "YEARS":
        if out.get("AGEU") != "YEARS":
            row_issues.append("AGE present but AGEU not standardized to YEARS")
            hard_fail = True

    if out.get("COUNTRY") is not None:
        if sponsor_rules.get("country_rule", {}).get("target_standard") == "ISO 3166-1 alpha-3" and len(str(out["COUNTRY"])) != 3:
            row_issues.append("COUNTRY not ISO alpha-3")
            hard_fail = True

    if out.get("DTHFL") == "Y" and not out.get("DTHDTC") and sponsor_rules.get("dthfl_rule", {}).get("set_Y_if_dthdtc_present", True):
        row_issues.append("DTHFL=Y but DTHDTC missing")
        hard_fail = True
    if out.get("DTHDTC") and out.get("DTHFL") != "Y":
        row_issues.append("DTHDTC populated but DTHFL not Y")
        hard_fail = True

    b = partial_sort_key(out.get("BRTHDTC"))
    r = partial_sort_key(out.get("RFSTDTC"))
    d = partial_sort_key(out.get("DTHDTC"))
    if sponsor_rules.get("date_rule", {}).get("preserve_partial_dates", True):
        if b and r and r < b:
            row_issues.append("RFSTDTC before BRTHDTC")
            hard_fail = True
        if r and d and d < r:
            row_issues.append("DTHDTC before RFSTDTC")
            hard_fail = True
        if out.get("BRTHDTC") and len(str(out["BRTHDTC"])) in {4, 7}:
            row_issues.append("BRTHDTC partial date requires human review")
            hard_fail = True
        if out.get("RFSTDTC") and len(str(out["RFSTDTC"])) in {4, 7}:
            row_issues.append("RFSTDTC partial date requires human review")
            hard_fail = True
        if out.get("DTHDTC") and len(str(out["DTHDTC"])) in {4, 7}:
            row_issues.append("DTHDTC partial date requires human review")
            hard_fail = True

    allowed_loss_fields = set(sponsor_rules.get("allowed_intentional_loss_fields", ["DTHFL"]))
    loss_fields = []
    for c in FINAL_COLUMNS:
        before = original.get(c)
        after = out.get(c)
        if before not in [None, ""] and after in [None, ""]:
            if c in allowed_loss_fields:
                continue
            if c == "DTHDTC" and out.get("DTHFL") != "Y":
                continue
            loss_fields.append(c)
    if loss_fields:
        row_issues.append("Unexpected data loss in fields: " + ", ".join(loss_fields))
        hard_fail = True

    if rownum in rows_with_human_issues(human_df):
        row_issues.append("Row has HUMAN_REVIEW issues from Layer 1; excluded from final SDTM until resolved")
        hard_fail = True

    if has_human_field_issue(rownum, "ARM", human_df) or has_human_field_issue(rownum, "ACTARM", human_df):
        row_issues.append("ARM/ACTARM has HUMAN_REVIEW issue from Layer 1; excluded from final SDTM until resolved")
        hard_fail = True

    unresolved_sdtm = []
    if rownum in rows_with_sdtm_fixable_issues(sdtm_fixable_df):
        if should_apply_sdtm_fix(rownum, "AGE", sdtm_fixable_df, rows_with_sdtm_fixable_issues(sdtm_fixable_df)):
            if not out.get("AGE"):
                unresolved_sdtm.append("AGE unresolved")
        if should_apply_sdtm_fix(rownum, "AGEU", sdtm_fixable_df, rows_with_sdtm_fixable_issues(sdtm_fixable_df)):
            if not out.get("AGEU"):
                unresolved_sdtm.append("AGEU unresolved")
        if should_apply_sdtm_fix(rownum, "USUBJID", sdtm_fixable_df, rows_with_sdtm_fixable_issues(sdtm_fixable_df)):
            if not out.get("USUBJID"):
                unresolved_sdtm.append("USUBJID unresolved")

    if unresolved_sdtm:
        row_issues.append("Unresolved SDTM-fixable issues: " + ", ".join(unresolved_sdtm))
        hard_fail = True

    return row_issues, hard_fail


def build_dm(
    source_df: pd.DataFrame,
    ct_lut: Dict[tuple, str],
    study_meta: dict,
    prog_conv: dict,
    sponsor_rules: dict,
    human_df: pd.DataFrame,
    sdtm_fixable_df: pd.DataFrame,
):
    final_rows = []
    exception_rows = []
    qc_rows = []
    auto_rows = []

    source_df = make_row_reference_df(source_df)
    sdtm_fixable_rows = apply_sdtm_fixable_gate(source_df, sdtm_fixable_df)

    for _, src_row in source_df.iterrows():
        rownum = int(src_row["L1_SOURCE_ROW_NUMBER"])

        out, pre_issues, auto_actions = build_dm_row(
            src_row=src_row,
            rownum=rownum,
            ct_lut=ct_lut,
            study_meta=study_meta,
            prog_conv=prog_conv,
            sponsor_rules=sponsor_rules,
            sdtm_fixable_df=sdtm_fixable_df,
            sdtm_fixable_rows=sdtm_fixable_rows,
        )

        original = {c: get_row_value(src_row, c) for c in FINAL_COLUMNS}
        row_issues, hard_fail = validate_dm_row(
            out=out,
            original=original,
            rownum=rownum,
            sponsor_rules=sponsor_rules,
            sdtm_fixable_df=sdtm_fixable_df,
            human_df=human_df,
        )

        row_issues = pre_issues + row_issues

        exception_record = {c: out.get(c) for c in FINAL_COLUMNS}
        exception_record["SOURCE_ROW_NUMBER"] = rownum
        exception_record["ROW_ISSUES"] = " | ".join(row_issues)

        qc_rows.append({
            "SOURCE_ROW_NUMBER": rownum,
            "USUBJID": out.get("USUBJID"),
            "ISSUE_COUNT": len(row_issues),
            "ISSUES": " | ".join(row_issues),
            "DISPOSITION": "EXCEPTION" if hard_fail else ("PASS_WITH_WARNINGS" if row_issues else "PASS"),
        })

        if auto_actions or rownum in sdtm_fixable_rows:
            auto_rows.append({
                "SOURCE_ROW_NUMBER": rownum,
                "AUTO_ACTIONS": " | ".join(auto_actions) if auto_actions else "Row had Layer 1 SDTM-standardizable issue(s); no deterministic action was required."
            })

        if hard_fail:
            exception_rows.append(exception_record)
        else:
            final_rows.append({c: out.get(c) for c in FINAL_COLUMNS})

    final_df = pd.DataFrame(final_rows)
    exceptions_df = pd.DataFrame(exception_rows)
    qc_df = pd.DataFrame(qc_rows)
    auto_df = pd.DataFrame(auto_rows)

    if final_df.empty:
        final_df = pd.DataFrame(columns=FINAL_COLUMNS)
    else:
        final_df = final_df.reindex(columns=FINAL_COLUMNS)

    if exceptions_df.empty:
        exceptions_df = pd.DataFrame(columns=EXCEPTION_COLUMNS)
    else:
        exceptions_df = exceptions_df.reindex(columns=EXCEPTION_COLUMNS)

    if qc_df.empty:
        qc_df = pd.DataFrame(columns=["SOURCE_ROW_NUMBER", "USUBJID", "ISSUE_COUNT", "ISSUES", "DISPOSITION"])

    if auto_df.empty:
        auto_df = pd.DataFrame(columns=AUTO_LOG_COLUMNS)
    else:
        auto_df = auto_df.reindex(columns=AUTO_LOG_COLUMNS)

    if not final_df.empty and "USUBJID" in final_df.columns:
        dup_mask = final_df["USUBJID"].duplicated(keep=False)
        if dup_mask.any():
            dup_rows = final_df.loc[dup_mask].copy()
            dup_rows["SOURCE_ROW_NUMBER"] = ""
            dup_rows["ROW_ISSUES"] = "Duplicate USUBJID in final DM"
            exceptions_df = pd.concat([exceptions_df, dup_rows.reindex(columns=EXCEPTION_COLUMNS)], ignore_index=True)
            final_df = final_df.loc[~dup_mask].copy()
            final_df = final_df.reindex(columns=FINAL_COLUMNS)

    return final_df, exceptions_df, qc_df, auto_df


def build_summary_df(
    source_df: pd.DataFrame,
    candidate_df: pd.DataFrame,
    human_rejected_df: pd.DataFrame,
    final_df: pd.DataFrame,
    exceptions_df: pd.DataFrame,
    auto_df: pd.DataFrame,
) -> pd.DataFrame:
    rows = [
        {"metric": "layer1_clean_input_rows", "value": len(source_df)},
        {"metric": "rows_rejected_by_layer1_human", "value": len(human_rejected_df)},
        {"metric": "rows_after_human_gate", "value": len(candidate_df)},
        {"metric": "rows_routed_to_sdtm_exceptions", "value": len(exceptions_df)},
        {"metric": "final_dm_rows", "value": len(final_df)},
        {"metric": "rows_with_auto_actions_logged", "value": len(auto_df)},
    ]
    return pd.DataFrame(rows, columns=BUILD_SUMMARY_COLUMNS)


def write_outputs(
    final_df: pd.DataFrame,
    exceptions_df: pd.DataFrame,
    qc_df: pd.DataFrame,
    auto_df: pd.DataFrame,
    human_rejected_df: pd.DataFrame,
    build_summary_df_: pd.DataFrame,
    detected_inputs_df: pd.DataFrame,
    used: dict,
    spec_df: pd.DataFrame,
):
    OUTPUT_DIR.mkdir(exist_ok=True)

    ordered_cols = spec_target_order(spec_df)
    final_ordered_df = final_df.reindex(columns=ordered_cols)
    final_df = final_df.reindex(columns=FINAL_COLUMNS)

    if human_rejected_df.empty:
        human_out = pd.DataFrame(columns=list(human_rejected_df.columns) if len(human_rejected_df.columns) else ["L1_SOURCE_ROW_NUMBER"])
    else:
        human_out = human_rejected_df.copy()

    final_df.to_csv(OUTPUT_DIR / "dm_final_sdtm.csv", index=False)
    final_ordered_df.to_csv(OUTPUT_DIR / "dm_final_sdtm_ordered.csv", index=False)
    exceptions_df.to_csv(OUTPUT_DIR / "dm_exceptions_transform.csv", index=False)
    qc_df.to_csv(OUTPUT_DIR / "dm_sdtm_qc_report.csv", index=False)
    auto_df.to_csv(OUTPUT_DIR / "dm_auto_standardized_log.csv", index=False)
    human_out.to_csv(OUTPUT_DIR / "dm_exceptions_human.csv", index=False)
    build_summary_df_.to_csv(OUTPUT_DIR / "dm_build_summary.csv", index=False)
    detected_inputs_df.to_csv(OUTPUT_DIR / "dm_detected_inputs.csv", index=False)

    readme = (
        "SDTM DM LB-style build notes:\n"
        "- Starts from Layer 1 cleaned DM source.\n"
        "- Uses Layer 1 human-review issue file as a hard gate before final eligibility.\n"
        "- Uses Layer 1 SDTM-fixable issue file to control which downstream deterministic fixes are allowed.\n"
        "- Preserves original helper functions from the earlier DM script and adds LB-style input flow and output separation.\n"
        "- Final dataset contains only CDISC DM variables.\n\n"
        f"Layer 1 source used: {used['clean_source'].name if used.get('clean_source') else 'N/A'}\n"
        f"Raw source used: {used['raw_source'].name if used.get('raw_source') else 'N/A'}\n"
        f"Controlled terminology used: {used['ct'].name if used.get('ct') else 'N/A'}\n"
        f"Study metadata used: {used['meta'].name if used.get('meta') else 'N/A'}\n"
        f"Programming conventions used: {used['prog'].name if used.get('prog') else 'N/A'}\n"
        f"Sponsor rules used: {used['sponsor_rules'].name if used.get('sponsor_rules') else 'N/A'}\n"
        f"Human issues file used: {used['human_issues'].name if used.get('human_issues') else 'N/A'}\n"
        f"SDTM-fixable issues file used: {used['sdtm_fixable_issues'].name if used.get('sdtm_fixable_issues') else 'N/A'}\n"
        f"Spec file used: {used['spec'].name if used.get('spec') else 'N/A'}\n"
    )
    (OUTPUT_DIR / "README.txt").write_text(readme, encoding="utf-8")


def main():
    project_folder = BASE
    source_df, raw_df, ct_df, study_meta, prog_conv, sponsor_rules, human_df, sdtm_fixable_df, spec_df, used = load_inputs(project_folder)

    source_df = normalize_df(source_df, prog_conv)
    if not raw_df.empty:
        raw_df = normalize_df(raw_df, prog_conv)

    candidate_df, human_rejected_df = apply_human_issue_gate(source_df, human_df)
    ct_lut = make_ct_lookup(ct_df)

    final_df, exceptions_df, qc_df, auto_df = build_dm(
        source_df=candidate_df,
        ct_lut=ct_lut,
        study_meta=study_meta,
        prog_conv=prog_conv,
        sponsor_rules=sponsor_rules,
        human_df=human_df,
        sdtm_fixable_df=sdtm_fixable_df,
    )

    build_summary_df_ = build_summary_df(
        source_df=source_df,
        candidate_df=candidate_df,
        human_rejected_df=human_rejected_df,
        final_df=final_df,
        exceptions_df=exceptions_df,
        auto_df=auto_df,
    )

    detected_inputs_df = pd.DataFrame([
        {"file_role": k, "path": str(v) if v is not None else ""}
        for k, v in used.items()
    ])

    write_outputs(
        final_df=final_df,
        exceptions_df=exceptions_df,
        qc_df=qc_df,
        auto_df=auto_df,
        human_rejected_df=human_rejected_df,
        build_summary_df_=build_summary_df_,
        detected_inputs_df=detected_inputs_df,
        used=used,
        spec_df=spec_df,
    )

    print(f"Created outputs in: {OUTPUT_DIR}")
    print("- dm_final_sdtm.csv")
    print("- dm_final_sdtm_ordered.csv")
    print("- dm_exceptions_transform.csv")
    print("- dm_exceptions_human.csv")
    print("- dm_sdtm_qc_report.csv")
    print("- dm_auto_standardized_log.csv")
    print("- dm_build_summary.csv")
    print("- dm_detected_inputs.csv")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
