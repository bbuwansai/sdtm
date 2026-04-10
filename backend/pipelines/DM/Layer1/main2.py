import pandas as pd
import numpy as np
import re
from pathlib import Path

TEXT_COLS = [
    "STUDYID", "DOMAIN", "USUBJID", "SUBJID", "SITEID", "SEX", "RACE", "ETHNIC",
    "COUNTRY", "ARM", "ACTARM", "AGEU", "DTHFL", "DTHDTC", "RFSTDTC", "BRTHDTC"
]

SEX_MAP = {"male": "M", "m": "M", "female": "F", "f": "F", "u": "U", "unknown": "U"}
AGEU_MAP = {"years": "YEARS", "yrs": "YEARS", "yr": "YEARS", "y": "YEARS"}
DTHFL_MAP = {"yes": "Y", "no": "N", "y": "Y", "n": "N"}
COUNTRY_MAP = {"INDIA": "IND", "UNITED STATES": "USA", "UK": "GBR"}


def normalize_text(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    return x if x != "" else None


def normalize_siteid(x):
    x = normalize_text(x)
    if x is None:
        return None
    if re.fullmatch(r"\d+\.0", x):
        x = x[:-2]
    if re.fullmatch(r"\d+", x):
        return x.zfill(3)
    return x


def normalize_subjid(x):
    x = normalize_text(x)
    if x is None:
        return None
    if re.fullmatch(r"\d+\.0", x):
        x = x[:-2]
    return x


def parse_partial_date(s):
    s = normalize_text(s)
    if s is None:
        return {"raw": None, "granularity": None, "date": pd.NaT, "valid": True}

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        dt = pd.to_datetime(s, errors="coerce")
        return {"raw": s, "granularity": "DAY", "date": dt, "valid": pd.notna(dt)}

    if re.fullmatch(r"\d{4}-\d{2}", s):
        _, month = map(int, s.split("-"))
        return {"raw": s, "granularity": "MONTH", "date": pd.NaT, "valid": 1 <= month <= 12}

    if re.fullmatch(r"\d{4}", s):
        return {"raw": s, "granularity": "YEAR", "date": pd.NaT, "valid": True}

    return {"raw": s, "granularity": "INVALID", "date": pd.NaT, "valid": False}


def infer_studyid(series):
    vals = [normalize_text(v) for v in series.tolist()]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return pd.Series(vals).mode().iloc[0]


def derive_usubjid(studyid, siteid, subjid):
    studyid = normalize_text(studyid)
    siteid = normalize_siteid(siteid)
    subjid = normalize_subjid(subjid)
    if studyid and siteid and subjid:
        return f"{studyid}-{siteid}-{subjid}"
    return None


def parse_usubjid(usubjid):
    usubjid = normalize_text(usubjid)
    if usubjid is None:
        return None
    parts = usubjid.split("-")
    if len(parts) != 3:
        return None
    return {
        "STUDYID": normalize_text(parts[0]),
        "SITEID": normalize_siteid(parts[1]),
        "SUBJID": normalize_subjid(parts[2]),
    }


def add_issue(issues, rec_id, field, severity, category, rule_id, rule_description,
              observed, expected=None, auto_fix_applied="N"):
    issues.append({
        "row_num": rec_id,
        "field": field,
        "severity": severity,
        "category": category,
        "rule_id": rule_id,
        "rule_description": rule_description,
        "observed": observed,
        "expected": expected,
        "auto_fix_applied": auto_fix_applied
    })


def row_disposition_for_severities(severities):
    if "CRITICAL" in severities:
        return "FAIL_REVIEW_REQUIRED"
    if "WARNING" in severities:
        return "PASS_WITH_WARNINGS"
    return "PASS"


def clean_dm(df):
    issues = []
    clean = df.copy()

    expected_cols = [
        "USUBJID", "STUDYID", "SITEID", "SUBJID", "DOMAIN",
        "RFSTDTC", "BRTHDTC", "DTHFL", "DTHDTC",
        "SEX", "AGE", "AGEU", "COUNTRY", "ARM", "ACTARM"
    ]
    for col in expected_cols:
        if col not in clean.columns:
            clean[col] = None

    # Keep identifiers / coded fields / dates as text
    for col in TEXT_COLS:
        if col in clean.columns:
            clean[col] = clean[col].apply(normalize_text)

    # AGE is intentionally numeric for downstream validation and derivation checks
    if "AGE" in clean.columns:
        clean["AGE"] = pd.to_numeric(clean["AGE"], errors="coerce").astype("Float64")

    # Normalize identifiers explicitly
    if "SITEID" in clean.columns:
        clean["SITEID"] = clean["SITEID"].apply(normalize_siteid)
    if "SUBJID" in clean.columns:
        clean["SUBJID"] = clean["SUBJID"].apply(normalize_subjid)
    if "USUBJID" in clean.columns:
        clean["USUBJID"] = clean["USUBJID"].apply(normalize_text)

    inferred_studyid = infer_studyid(clean["STUDYID"]) if "STUDYID" in clean.columns else None

    # 1. Fill STUDYID when missing, and flag inconsistent STUDYID values
    if inferred_studyid is not None:
        for idx in clean.index:
            rec_id = idx + 1
            current = clean.at[idx, "STUDYID"]
            if current is None:
                clean.at[idx, "STUDYID"] = inferred_studyid
                add_issue(
                    issues, rec_id, "STUDYID", "INFO", "AUTO_FILL",
                    "DM001", "Missing STUDYID filled from study-level constant",
                    None, inferred_studyid, "Y"
                )
            elif current != inferred_studyid:
                add_issue(
                    issues, rec_id, "STUDYID", "CRITICAL", "CONSISTENCY",
                    "DM002", "STUDYID inconsistent within single study extract",
                    current, inferred_studyid, "N"
                )

    # 2. Normalize DOMAIN to DM for the whole file
    for idx in clean.index:
        rec_id = idx + 1
        original_domain = normalize_text(df.loc[idx, "DOMAIN"]) if "DOMAIN" in df.columns else None
        if original_domain != "DM":
            add_issue(
                issues, rec_id, "DOMAIN", "INFO", "STANDARDIZATION",
                "DM003", "DOMAIN normalized to DM",
                original_domain, "DM", "Y"
            )
    clean["DOMAIN"] = "DM"

    # 3. Standardize selected coded values and validate AGE
    for idx in clean.index:
        rec_id = idx + 1

        sex = normalize_text(clean.at[idx, "SEX"]) if "SEX" in clean.columns else None
        if sex is None:
            add_issue(
                issues, rec_id, "SEX", "WARNING", "MISSING_REQUIRED",
                "DM004", "SEX missing", None, "M/F/U", "N"
            )
        else:
            sex_std = SEX_MAP.get(sex.lower(), sex.upper())
            if sex_std in {"M", "F", "U"}:
                if sex != sex_std:
                    clean.at[idx, "SEX"] = sex_std
                    add_issue(
                        issues, rec_id, "SEX", "INFO", "STANDARDIZATION",
                        "DM005", "SEX standardized to coded value",
                        sex, sex_std, "Y"
                    )
            else:
                add_issue(
                    issues, rec_id, "SEX", "WARNING", "INVALID_CODE",
                    "DM006", "SEX not in allowed coded values",
                    sex, "M/F/U", "N"
                )

        original_age = df.loc[idx, "AGE"] if "AGE" in df.columns else None
        age_num = clean.at[idx, "AGE"] if "AGE" in clean.columns else pd.NA

        if pd.isna(age_num):
            add_issue(
                issues, rec_id, "AGE", "WARNING", "INVALID_FORMAT",
                "DM007", "AGE missing or non-numeric",
                original_age, "numeric", "N"
            )
        else:
            if age_num < 18:
                add_issue(
                    issues, rec_id, "AGE", "CRITICAL", "IMPLAUSIBLE_VALUE",
                    "DM008", "AGE below adult threshold",
                    original_age, ">=18", "N"
                )
            if age_num > 120:
                add_issue(
                    issues, rec_id, "AGE", "CRITICAL", "IMPLAUSIBLE_VALUE",
                    "DM009", "AGE implausibly high",
                    original_age, "<=120", "N"
                )

        ageu = normalize_text(clean.at[idx, "AGEU"]) if "AGEU" in clean.columns else None
        if ageu is None:
            add_issue(
                issues, rec_id, "AGEU", "WARNING", "MISSING_REQUIRED",
                "DM010", "AGEU missing", None, "YEARS", "N"
            )
        else:
            ageu_std = AGEU_MAP.get(ageu.lower(), ageu.upper())
            if ageu_std == "YEARS":
                if ageu != ageu_std:
                    clean.at[idx, "AGEU"] = "YEARS"
                    add_issue(
                        issues, rec_id, "AGEU", "INFO", "STANDARDIZATION",
                        "DM011", "AGEU standardized to YEARS",
                        ageu, "YEARS", "Y"
                    )
            else:
                add_issue(
                    issues, rec_id, "AGEU", "WARNING", "INVALID_CODE",
                    "DM012", "AGEU has unexpected value",
                    ageu, "YEARS", "N"
                )

        dthfl = normalize_text(clean.at[idx, "DTHFL"]) if "DTHFL" in clean.columns else None
        if dthfl is None:
            add_issue(
                issues, rec_id, "DTHFL", "WARNING", "MISSING_REQUIRED",
                "DM013", "DTHFL missing", None, "Y/N", "N"
            )
        else:
            dthfl_std = DTHFL_MAP.get(dthfl.lower(), dthfl.upper())
            if dthfl_std in {"Y", "N"}:
                if dthfl != dthfl_std:
                    clean.at[idx, "DTHFL"] = dthfl_std
                    add_issue(
                        issues, rec_id, "DTHFL", "INFO", "STANDARDIZATION",
                        "DM014", "DTHFL standardized to coded value",
                        dthfl, dthfl_std, "Y"
                    )
            else:
                add_issue(
                    issues, rec_id, "DTHFL", "WARNING", "INVALID_CODE",
                    "DM015", "DTHFL has invalid coded value",
                    dthfl, "Y/N", "N"
                )

        country = normalize_text(clean.at[idx, "COUNTRY"]) if "COUNTRY" in clean.columns else None
        country_key = country.upper() if isinstance(country, str) else country
        if country_key in COUNTRY_MAP:
            clean.at[idx, "COUNTRY"] = COUNTRY_MAP[country_key]
            add_issue(
                issues, rec_id, "COUNTRY", "INFO", "STANDARDIZATION",
                "DM016", "COUNTRY standardized",
                country, COUNTRY_MAP[country_key], "Y"
            )

    # 4. Recover SITEID/SUBJID from existing USUBJID when possible
    for idx in clean.index:
        rec_id = idx + 1
        parsed = parse_usubjid(clean.at[idx, "USUBJID"])
        if parsed is None:
            continue

        if clean.at[idx, "SITEID"] is None and parsed["SITEID"] is not None:
            clean.at[idx, "SITEID"] = parsed["SITEID"]
            add_issue(
                issues, rec_id, "SITEID", "INFO", "AUTO_FILL",
                "DM017A", "SITEID filled from existing USUBJID",
                None, parsed["SITEID"], "Y"
            )

        if clean.at[idx, "SUBJID"] is None and parsed["SUBJID"] is not None:
            clean.at[idx, "SUBJID"] = parsed["SUBJID"]
            add_issue(
                issues, rec_id, "SUBJID", "INFO", "AUTO_FILL",
                "DM017B", "SUBJID filled from existing USUBJID",
                None, parsed["SUBJID"], "Y"
            )

    # 5. Reconstruct missing USUBJID after STUDYID normalization
    for idx in clean.index:
        rec_id = idx + 1
        if clean.at[idx, "USUBJID"] is None:
            new_usubjid = derive_usubjid(
                clean.at[idx, "STUDYID"], clean.at[idx, "SITEID"], clean.at[idx, "SUBJID"]
            )
            if new_usubjid is not None:
                clean.at[idx, "USUBJID"] = new_usubjid
                add_issue(
                    issues, rec_id, "USUBJID", "INFO", "AUTO_FILL",
                    "DM017", "USUBJID reconstructed from STUDYID/SITEID/SUBJID",
                    None, new_usubjid, "Y"
                )

    # 6. Check existing USUBJID components match STUDYID/SITEID/SUBJID
    for idx in clean.index:
        rec_id = idx + 1
        usubjid = clean.at[idx, "USUBJID"]
        parsed = parse_usubjid(usubjid)
        if usubjid is not None and parsed is None:
            add_issue(
                issues, rec_id, "USUBJID", "CRITICAL", "INVALID_FORMAT",
                "DM017C", "USUBJID format invalid; expected STUDYID-SITEID-SUBJID",
                usubjid, "AAA-001-1001", "N"
            )
            continue

        if parsed is not None:
            expected_usubjid = derive_usubjid(
                clean.at[idx, "STUDYID"], clean.at[idx, "SITEID"], clean.at[idx, "SUBJID"]
            )
            if expected_usubjid is not None and usubjid != expected_usubjid:
                add_issue(
                    issues, rec_id, "USUBJID", "CRITICAL", "CROSS_FIELD_INCONSISTENCY",
                    "DM017D", "USUBJID does not match STUDYID/SITEID/SUBJID",
                    usubjid, expected_usubjid, "N"
                )

    # 7. Required-field checks after allowed hard fixes
    required_fields = [
        "STUDYID", "DOMAIN", "USUBJID", "SUBJID", "SITEID",
        "RFSTDTC", "ARM", "AGE", "ACTARM"
    ]

    for idx in clean.index:
        rec_id = idx + 1
        for col in required_fields:
            if normalize_text(clean.at[idx, col]) is None:
                add_issue(
                    issues, rec_id, col, "CRITICAL", "MISSING_REQUIRED",
                    "DM018", f"{col} is required but missing",
                    None, "must be populated", "N"
                )

        arm = normalize_text(clean.at[idx, "ARM"]) if "ARM" in clean.columns else None
        actarm = normalize_text(clean.at[idx, "ACTARM"]) if "ACTARM" in clean.columns else None

        if arm is None and actarm is not None:
            add_issue(
                issues, rec_id, "ARM", "WARNING", "CROSS_FIELD_INCONSISTENCY",
                "DM018A", "ARM is missing while ACTARM is populated",
                None, actarm, "N"
            )

        if arm is not None and actarm is None:
            add_issue(
                issues, rec_id, "ACTARM", "WARNING", "CROSS_FIELD_INCONSISTENCY",
                "DM018B", "ACTARM is missing while ARM is populated",
                arm, "ACTARM should usually be populated", "N"
            )

    # 8. Duplicate USUBJID after reconstruction
    dupes = clean["USUBJID"][clean["USUBJID"].notna() & clean["USUBJID"].duplicated(keep=False)]
    for idx, val in dupes.items():
        add_issue(
            issues, idx + 1, "USUBJID", "CRITICAL", "UNIQUENESS_VIOLATION",
            "DM019", "Duplicate USUBJID found",
            val, "must be unique", "N"
        )

    # 9. Date and cross-field checks
    for idx in clean.index:
        rec_id = idx + 1
        rf = parse_partial_date(clean.at[idx, "RFSTDTC"])
        br = parse_partial_date(clean.at[idx, "BRTHDTC"])
        dd = parse_partial_date(clean.at[idx, "DTHDTC"])

        for field, parsed in [("RFSTDTC", rf), ("BRTHDTC", br), ("DTHDTC", dd)]:
            if parsed["granularity"] == "INVALID":
                add_issue(
                    issues, rec_id, field, "CRITICAL", "INVALID_FORMAT",
                    "DM020", f"{field} has invalid date format",
                    clean.at[idx, field], "YYYY or YYYY-MM or YYYY-MM-DD", "N"
                )
            elif parsed["granularity"] in {"MONTH", "YEAR"}:
                add_issue(
                    issues, rec_id, field, "WARNING", "PARTIAL_DATE",
                    "DM021", f"{field} is partial and retained as-is",
                    clean.at[idx, field], parsed["granularity"], "N"
                )

        if pd.notna(rf["date"]) and pd.notna(br["date"]) and rf["date"] < br["date"]:
            add_issue(
                issues, rec_id, "RFSTDTC", "CRITICAL", "CROSS_FIELD_INCONSISTENCY",
                "DM022", "RFSTDTC occurs before BRTHDTC",
                clean.at[idx, "RFSTDTC"], f"after {clean.at[idx, 'BRTHDTC']}", "N"
            )

        if pd.notna(dd["date"]) and pd.notna(br["date"]) and dd["date"] < br["date"]:
            add_issue(
                issues, rec_id, "DTHDTC", "CRITICAL", "CROSS_FIELD_INCONSISTENCY",
                "DM022A", "DTHDTC occurs before BRTHDTC",
                clean.at[idx, "DTHDTC"], f"after {clean.at[idx, 'BRTHDTC']}", "N"
            )

        if pd.notna(dd["date"]) and pd.notna(rf["date"]) and dd["date"] < rf["date"]:
            add_issue(
                issues, rec_id, "DTHDTC", "WARNING", "DATE_CHRONOLOGY",
                "DM022B", "DTHDTC occurs before RFSTDTC",
                clean.at[idx, "DTHDTC"], f"on/after {clean.at[idx, 'RFSTDTC']}", "N"
            )

        age_num = clean.at[idx, "AGE"] if "AGE" in clean.columns else pd.NA
        if pd.notna(rf["date"]) and pd.notna(br["date"]) and pd.notna(age_num):
            calc_age = int((rf["date"] - br["date"]).days / 365.25)
            if abs(calc_age - float(age_num)) > 2:
                add_issue(
                    issues, rec_id, "AGE", "WARNING", "DERIVATION_MISMATCH",
                    "DM023", "AGE inconsistent with BRTHDTC and RFSTDTC",
                    float(age_num), f"approx {calc_age}", "N"
                )

        dthfl = normalize_text(clean.at[idx, "DTHFL"])
        dthdtc = normalize_text(clean.at[idx, "DTHDTC"])
        if dthfl == "N" and dthdtc is not None:
            add_issue(
                issues, rec_id, "DTHDTC", "CRITICAL", "CROSS_FIELD_INCONSISTENCY",
                "DM024", "DTHFL=N but DTHDTC is populated",
                dthdtc, "blank expected", "N"
            )
        if dthfl == "Y" and dthdtc is None:
            add_issue(
                issues, rec_id, "DTHDTC", "CRITICAL", "CROSS_FIELD_INCONSISTENCY",
                "DM025", "DTHFL=Y but DTHDTC is missing",
                None, "date expected", "N"
            )

    issues_df = pd.DataFrame(issues)

    severity_rank = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
    if not issues_df.empty:
        issues_df["severity_rank"] = issues_df["severity"].map(severity_rank)
        issues_df = (
            issues_df
            .sort_values(["severity_rank", "category", "row_num", "field", "rule_id"])
            .drop(columns=["severity_rank"])
            .reset_index(drop=True)
        )

    sev_map = issues_df.groupby("row_num")["severity"].agg(list).to_dict() if not issues_df.empty else {}
    clean["ROW_DISPOSITION"] = [
        row_disposition_for_severities(sev_map.get(idx + 1, []))
        for idx in clean.index
    ]
    return clean, issues_df


def main():
    script_dir = Path(__file__).resolve().parent

    candidates = [
        script_dir / "dm_source_50_rows.csv",
        script_dir / "dm_raw_crf_style_50_rows.csv",
        script_dir / "dm_raw_demo_50_rows.csv",
        script_dir / "dm_raw.csv",
    ]
    source_csv = next((p for p in candidates if p.exists()), None)

    if source_csv is None:
        raise FileNotFoundError(
            f"No DM source file found in {script_dir}. "
            f"Tried: {[str(p.name) for p in candidates]}"
        )

    # Read raw as text so identifiers like 001 stay intact
    df = pd.read_csv(source_csv, dtype=str)

    clean_df, issues_df = clean_dm(df)

    severity_summary_df = issues_df.groupby("severity").size().reset_index(name="issue_count")
    category_summary_df = issues_df.groupby(["severity", "category"]).size().reset_index(name="issue_count")
    rule_summary_df = (
        issues_df.groupby(["rule_id", "rule_description"])
        .size()
        .reset_index(name="issue_count")
        .sort_values("issue_count", ascending=False)
    )

    clean_path = script_dir / "dm_cleaned_output.csv"
    issues_path = script_dir / "dm_issue_log.csv"
    severity_path = script_dir / "dm_issue_summary_by_severity.csv"
    category_path = script_dir / "dm_issue_summary_by_category.csv"
    rule_path = script_dir / "dm_issue_summary_by_rule.csv"

    clean_df.to_csv(clean_path, index=False)
    issues_df.to_csv(issues_path, index=False)
    severity_summary_df.to_csv(severity_path, index=False)
    category_summary_df.to_csv(category_path, index=False)
    rule_summary_df.to_csv(rule_path, index=False)

    print("Generated files in same folder as script/source:")
    print(f"  - {clean_path.name}")
    print(f"  - {issues_path.name}")
    print(f"  - {severity_path.name}")
    print(f"  - {category_path.name}")
    print(f"  - {rule_path.name}")


if __name__ == "__main__":
    main()
