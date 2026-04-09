
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parent
OUTDIR = BASE / "vs_spec_outputs_v3"

def build_main_spec():
    rows = [
        {
            "spec_seq": 1, "target_domain": "VS", "target_variable": "STUDYID",
            "target_label": "Study Identifier", "target_type": "Char", "target_length": 20,
            "core": "Req", "origin": "CRF/eCRF", "mapping_class": "Direct",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "PROTOCOL_ID",
            "source_label": "Protocol Identifier", "source_type_hint": "char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "",
            "reconciliation_source": "",
            "rule": "Map STUDYID = trim(PROTOCOL_ID). Preserve collected study identifier as character.",
            "primary_qc_checks": "PROTOCOL_ID must not be missing.",
            "traceability_note": "Direct trace to Vital Signs CRF field PROTOCOL_ID.",
            "programming_notes": "No transformation beyond trim and character preservation.",
            "exception_condition": "Missing STUDYID -> exception",
            "final_condition": "Non-missing",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 2, "target_domain": "VS", "target_variable": "DOMAIN",
            "target_label": "Domain Abbreviation", "target_type": "Char", "target_length": 2,
            "core": "Req", "origin": "Assigned", "mapping_class": "Constant",
            "source_form_or_module": "Not collected / Assigned", "source_variable": "N/A",
            "source_label": "N/A", "source_type_hint": "N/A", "source_role_in_rule": "Support",
            "controlled_terms_or_format": "", "reconciliation_source": "",
            "rule": 'Set DOMAIN = "VS" for all records.',
            "primary_qc_checks": 'DOMAIN must equal "VS" for every output row.',
            "traceability_note": "Assigned constant per VS domain build.",
            "programming_notes": "No source field is used.",
            "exception_condition": 'If DOMAIN is not "VS" -> exception',
            "final_condition": 'Always "VS"',
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 3, "target_domain": "VS", "target_variable": "USUBJID",
            "target_label": "Unique Subject Identifier", "target_type": "Char", "target_length": 40,
            "core": "Req", "origin": "Derived", "mapping_class": "Derived",
            "source_form_or_module": "Vital Signs CRF",
            "source_variable": "PROTOCOL_ID + SITE_NUMBER + SUBJECT_NUMBER",
            "source_label": "Protocol + Site + Subject", "source_type_hint": "char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "",
            "reconciliation_source": "SUBJECT_KEY",
            "rule": 'If SUBJECT_KEY is present and sponsor-compliant, preserve it. Otherwise derive USUBJID as PROTOCOL_ID || "-" || SITE_NUMBER || "-" || SUBJECT_NUMBER, preserving SITE_NUMBER and SUBJECT_NUMBER as character identifiers.',
            "primary_qc_checks": "USUBJID must not be missing. Final VS output must not contain duplicate subject/date/test keys.",
            "traceability_note": "Traceable to subject-identifying fields on the Vital Signs CRF.",
            "programming_notes": "Do not coerce SITE_NUMBER or SUBJECT_NUMBER to numeric if left-padding could be lost.",
            "exception_condition": "Missing or duplicate USUBJID -> exception",
            "final_condition": "Unique and non-missing",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 4, "target_domain": "VS", "target_variable": "VSDTC",
            "target_label": "Date/Time of Vital Signs Measurement", "target_type": "Char", "target_length": 20,
            "core": "Req", "origin": "CRF/eCRF", "mapping_class": "Recode",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VS_DATE",
            "source_label": "Vital Signs Date", "source_type_hint": "date-like char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "ISO 8601",
            "reconciliation_source": "VS_TIME",
            "rule": "If VS_DATE is a valid partial/full ISO-like date in the forms YYYY, YYYY-MM, or YYYY-MM-DD, map directly to VSDTC and preserve the collected granularity. Do not impute missing month or day. In this demo build, do not append VS_TIME to VSDTC; VS_TIME is reviewed separately in Layer 1 but not carried into final VSDTC.",
            "primary_qc_checks": "VSDTC must not be missing in final. Preserve valid partial dates. Invalid dates go to exceptions.",
            "traceability_note": "Direct trace to VS_DATE; VS_TIME is support/QC only in this demo.",
            "programming_notes": "No date imputation. Keep character representation.",
            "exception_condition": "Missing or invalid VSDTC -> exception",
            "final_condition": "Valid ISO 8601 full/partial date",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 5, "target_domain": "VS", "target_variable": "VISIT",
            "target_label": "Visit Name", "target_type": "Char", "target_length": 40,
            "core": "Perm", "origin": "CRF/eCRF", "mapping_class": "Direct",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VISIT_NAME",
            "source_label": "Visit Name", "source_type_hint": "char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_visit_map_v3.csv",
            "reconciliation_source": "VISIT_NUM",
            "rule": "Map VISIT = VISIT_NAME after trim. Preserve the sponsor-defined visit label. VISIT_NAME/VISIT_NUM consistency is validated against vs_visit_map_v3.csv; mismatches go to exception.",
            "primary_qc_checks": "VISIT_NAME and VISIT_NUM should be consistent with the sponsor visit map.",
            "traceability_note": "Direct trace to visit field on the Vital Signs CRF.",
            "programming_notes": "No recoding beyond trim in this demo build.",
            "exception_condition": "VISIT_NAME/VISIT_NUM mismatch -> exception",
            "final_condition": "Matches visit map",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 6, "target_domain": "VS", "target_variable": "VISITNUM",
            "target_label": "Visit Number", "target_type": "Num", "target_length": 8,
            "core": "Perm", "origin": "CRF/eCRF", "mapping_class": "Direct",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VISIT_NUM",
            "source_label": "Visit Number", "source_type_hint": "numeric-like char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_visit_map_v3.csv",
            "reconciliation_source": "VISIT_NAME",
            "rule": "Map VISITNUM = VISIT_NUM after numeric normalization where the value is numeric-like. Preserve the sponsor visit numbering convention. If VISIT_NAME and VISIT_NUM are inconsistent with vs_visit_map_v3.csv, route the row to exception rather than silently recoding the visit.",
            "primary_qc_checks": "VISITNUM should align with sponsor visit map and should be non-decreasing by subject visit chronology when dates are comparable.",
            "traceability_note": "Direct trace to visit numbering field on the Vital Signs CRF.",
            "programming_notes": "Do not manufacture VISITNUM from VISIT_NAME unless the sponsor explicitly allows it.",
            "exception_condition": "Non-numeric or visit-map mismatch -> exception",
            "final_condition": "Numeric and visit-map aligned",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 7, "target_domain": "VS", "target_variable": "VSTESTCD",
            "target_label": "Vital Signs Test Short Name", "target_type": "Char", "target_length": 8,
            "core": "Req", "origin": "Derived", "mapping_class": "Recode",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VS_TEST_RAW",
            "source_label": "Raw Vital Signs Test Name", "source_type_hint": "char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_test_map_v3.csv",
            "reconciliation_source": "",
            "rule": "Map VS_TEST_RAW to sponsor-approved VSTESTCD using vs_test_map_v3.csv. Unmapped raw test names go to exception. VSTESTCD values are sponsor-defined controlled terminology aligned to the CDISC-style VS test coding approach used in this demo.",
            "primary_qc_checks": "VSTESTCD must not be missing in final and must align one-to-one with VSTEST.",
            "traceability_note": "Traceable through VS test mapping table.",
            "programming_notes": "Use external test map sheet as deterministic lookup.",
            "exception_condition": "Unmapped test -> exception",
            "final_condition": "Mapped to approved code",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 8, "target_domain": "VS", "target_variable": "VSTEST",
            "target_label": "Vital Signs Test Name", "target_type": "Char", "target_length": 40,
            "core": "Req", "origin": "Derived", "mapping_class": "Recode",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VS_TEST_RAW",
            "source_label": "Raw Vital Signs Test Name", "source_type_hint": "char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_test_map_v3.csv",
            "reconciliation_source": "",
            "rule": "Map VS_TEST_RAW to sponsor-approved VSTEST using vs_test_map_v3.csv. Unmapped raw test names go to exception.",
            "primary_qc_checks": "VSTEST must not be missing in final and must align one-to-one with VSTESTCD.",
            "traceability_note": "Traceable through VS test mapping table.",
            "programming_notes": "Use external test map sheet as deterministic lookup.",
            "exception_condition": "Unmapped test -> exception",
            "final_condition": "Mapped to approved label",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 9, "target_domain": "VS", "target_variable": "VSORRES",
            "target_label": "Result or Finding in Original Units", "target_type": "Char", "target_length": 20,
            "core": "Req", "origin": "CRF/eCRF", "mapping_class": "Direct",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VS_RESULT_RAW",
            "source_label": "Raw Vital Signs Result", "source_type_hint": "char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "",
            "reconciliation_source": "",
            "rule": "Map VSORRES = VS_RESULT_RAW preserving the collected character representation. Do not convert units or numeric precision in VSORRES.",
            "primary_qc_checks": "VSORRES must not be missing in final. Numeric-only tests with non-numeric VSORRES should be routed to exceptions per VSSTRESN rules.",
            "traceability_note": "Direct trace to collected raw result field.",
            "programming_notes": "Preserve collected representation for auditability.",
            "exception_condition": "Missing VSORRES -> exception",
            "final_condition": "Non-missing",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 10, "target_domain": "VS", "target_variable": "VSORRESU",
            "target_label": "Original Units", "target_type": "Char", "target_length": 20,
            "core": "Perm", "origin": "CRF/eCRF", "mapping_class": "Recode",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VS_UNIT_RAW",
            "source_label": "Raw Vital Signs Unit", "source_type_hint": "char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_unit_map_v3.csv",
            "reconciliation_source": "VS_TEST_RAW",
            "rule": "Normalize VS_UNIT_RAW to a standard original-unit text representation using vs_unit_map_v3.csv without converting the physical value. Normalize synonyms only. Do not convert units here. If the raw unit is missing for a numeric VS test, or if the unit is unsupported for the mapped test, route the row to exception.",
            "primary_qc_checks": "VSORRESU should be present for numeric tests and must be allowed for the mapped test.",
            "traceability_note": "Traceable through unit normalization mapping sheet.",
            "programming_notes": "Normalize text only. No unit conversion in VSORRESU.",
            "exception_condition": "Unsupported unit or missing unit for numeric test -> exception",
            "final_condition": "Allowed normalized original unit",
            "review_status": "Final", "confidence": "High", "ambiguity_note": "Kelvin is allowed as a source unit for temperature in this v3 demo specification."
        },
        {
            "spec_seq": 11, "target_domain": "VS", "target_variable": "VSSTRESC",
            "target_label": "Character Result/Finding in Standard Format", "target_type": "Char", "target_length": 20,
            "core": "Exp", "origin": "Derived", "mapping_class": "Derived",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VS_RESULT_RAW + VS_UNIT_RAW + VS_TEST_RAW",
            "source_label": "Raw Result + Unit + Test", "source_type_hint": "mixed",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_conversion_rules_v3.csv",
            "reconciliation_source": "",
            "rule": "Derive VSSTRESC from the standardized result representation after applying sponsor-approved numeric conversion rules from vs_conversion_rules_v3.csv. If VSORRES is numeric and conversion is supported, set VSSTRESC to the character form of the standardized numeric result. Round converted TEMP/WEIGHT/HEIGHT values to 1 decimal place. If the row cannot be standardized because the result is non-numeric for a numeric test or the unit is unsupported, route the row to exception.",
            "primary_qc_checks": "VSSTRESC should align with VSSTRESN when numeric standardization succeeds.",
            "traceability_note": "Traceable through result conversion rules by test and unit.",
            "programming_notes": "Round converted TEMP/WEIGHT/HEIGHT values to 1 decimal place in this demo.",
            "exception_condition": "Non-numeric result for numeric test or unsupported unit -> exception",
            "final_condition": "Standardization succeeds",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 12, "target_domain": "VS", "target_variable": "VSSTRESN",
            "target_label": "Numeric Result/Finding in Standard Units", "target_type": "Num", "target_length": 8,
            "core": "Exp", "origin": "Derived", "mapping_class": "Derived",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VS_RESULT_RAW + VS_UNIT_RAW + VS_TEST_RAW",
            "source_label": "Raw Result + Unit + Test", "source_type_hint": "mixed",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_conversion_rules_v3.csv",
            "reconciliation_source": "",
            "rule": "Derive VSSTRESN only when VSORRES is numeric and the source unit is supported for the mapped test. Conversion rules in this demo are: TEMP F -> C as (F - 32) * 5 / 9; TEMP K -> C as K - 273.15; TEMP C stays in C; WEIGHT lb -> kg as lb * 0.45359237; WEIGHT kg stays in kg; HEIGHT in -> cm as in * 2.54; HEIGHT cm stays in cm; SYSBP and DIABP stay numeric in mmHg; PULSE stays numeric in beats/min. Round converted TEMP/WEIGHT/HEIGHT values to 1 decimal place. If result is non-numeric or unit is unsupported, route the row to exception.",
            "primary_qc_checks": "VSSTRESN must be numeric when populated and should be within sponsor plausibility range by test after standardization according to vs_implausibility_thresholds_v3.csv.",
            "traceability_note": "Traceable through explicit conversion formulas and source values.",
            "programming_notes": "Do not derive VSSTRESN for unsupported or non-numeric scenarios.",
            "exception_condition": "Non-numeric result or unsupported unit -> exception",
            "final_condition": "Numeric derivation succeeds and value is plausible",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 13, "target_domain": "VS", "target_variable": "VSSTRESU",
            "target_label": "Standard Units", "target_type": "Char", "target_length": 20,
            "core": "Exp", "origin": "Derived", "mapping_class": "Derived",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "VS_TEST_RAW + VS_UNIT_RAW",
            "source_label": "Raw Test + Raw Unit", "source_type_hint": "mixed",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_unit_map_v3.csv",
            "reconciliation_source": "",
            "rule": "Assign VSSTRESU by mapped test according to sponsor standard units from vs_unit_map_v3.csv. In this demo: SYSBP -> mmHg; DIABP -> mmHg; PULSE -> beats/min; TEMP -> C; WEIGHT -> kg; HEIGHT -> cm. If the row cannot be standardized because the source unit is unsupported for the mapped test, route the row to exception.",
            "primary_qc_checks": "VSSTRESU should be consistent with VSTESTCD and VSSTRESN.",
            "traceability_note": "Traceable through standard unit assignment rules by test.",
            "programming_notes": "Do not populate VSSTRESU when standardization is not supported.",
            "exception_condition": "Unsupported unit -> exception",
            "final_condition": "Standard unit determined",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 14, "target_domain": "VS", "target_variable": "VSPOS",
            "target_label": "Position of Subject During Measurement", "target_type": "Char", "target_length": 20,
            "core": "Perm", "origin": "CRF/eCRF", "mapping_class": "Recode",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "POSITION_RAW",
            "source_label": "Measurement Position", "source_type_hint": "char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_position_map_v3.csv or allowed values set",
            "reconciliation_source": "",
            "rule": "Map POSITION_RAW to VSPOS using the sponsor-controlled allowed values SITTING, SUPINE, and STANDING. If POSITION_RAW is missing, leave VSPOS blank. If POSITION_RAW contains an unsupported value, route the row to exception or QC according to sponsor policy.",
            "primary_qc_checks": "If populated, VSPOS must be in the allowed value set.",
            "traceability_note": "Direct trace to raw position field with controlled-value normalization.",
            "programming_notes": "No semantic conversion beyond normalization to the allowed set.",
            "exception_condition": "Unsupported value -> exception/QC",
            "final_condition": "Missing or allowed value",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 15, "target_domain": "VS", "target_variable": "VSFAST",
            "target_label": "Fasting Status", "target_type": "Char", "target_length": 1,
            "core": "Perm", "origin": "CRF/eCRF", "mapping_class": "Recode",
            "source_form_or_module": "Vital Signs CRF", "source_variable": "FASTING_RAW",
            "source_label": "Fasting Status Raw", "source_type_hint": "char",
            "source_role_in_rule": "Primary", "controlled_terms_or_format": "vs_fasting_map_v3.csv or allowed values set",
            "reconciliation_source": "",
            "rule": "Map FASTING_RAW to VSFAST using the sponsor-controlled values Y and N. If FASTING_RAW is missing, leave VSFAST blank. If FASTING_RAW contains an unsupported value, route the row to exception or QC according to sponsor policy. VSFAST follows a CDISC-style Y/N controlled-value pattern in this demo.",
            "primary_qc_checks": "If populated, VSFAST must equal Y or N.",
            "traceability_note": "Direct trace to raw fasting-status field with controlled-value normalization.",
            "programming_notes": "No semantic conversion beyond normalization to Y/N.",
            "exception_condition": "Unsupported value -> exception/QC",
            "final_condition": "Missing or Y/N",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
        {
            "spec_seq": 16, "target_domain": "VS", "target_variable": "VS_KEY",
            "target_label": "Operational Uniqueness Rule",
            "target_type": "Char", "target_length": 100,
            "core": "Perm", "origin": "Assigned", "mapping_class": "Support/QC only",
            "source_form_or_module": "Assigned", "source_variable": "N/A",
            "source_label": "N/A", "source_type_hint": "N/A",
            "source_role_in_rule": "Support", "controlled_terms_or_format": "",
            "reconciliation_source": "",
            "rule": "Operational uniqueness for duplicate handling in this demo is defined by STUDYID + USUBJID + VSTESTCD + VSDTC + VISITNUM. If duplicate rows occur on this key in final VS, route duplicates to exception.",
            "primary_qc_checks": "No duplicate final VS key combinations.",
            "traceability_note": "Support/QC rule only; not a submitted SDTM variable.",
            "programming_notes": "Used by QC and exception routing, not output to final SDTM.",
            "exception_condition": "Duplicate final key -> exception",
            "final_condition": "Not output",
            "review_status": "Final", "confidence": "High", "ambiguity_note": ""
        },
    ]
    return pd.DataFrame(rows)

def build_test_map():
    return pd.DataFrame([
        ["SYSTOLIC BLOOD PRESSURE", "SYSBP", "Systolic Blood Pressure"],
        ["SYST BP", "SYSBP", "Systolic Blood Pressure"],
        ["DIASTOLIC BLOOD PRESSURE", "DIABP", "Diastolic Blood Pressure"],
        ["PULSE RATE", "PULSE", "Pulse Rate"],
        ["TEMPERATURE", "TEMP", "Temperature"],
        ["WEIGHT", "WEIGHT", "Weight"],
        ["HEIGHT", "HEIGHT", "Height"],
    ], columns=["raw_test_name", "vstestcd", "vstest"])

def build_unit_map():
    return pd.DataFrame([
        ["SYSBP", "mmHg", "mmHg", "mmHg", "no conversion"],
        ["SYSBP", "MMHG", "mmHg", "mmHg", "no conversion"],
        ["SYSBP", "mm hg", "mmHg", "mmHg", "no conversion"],
        ["DIABP", "mmHg", "mmHg", "mmHg", "no conversion"],
        ["DIABP", "MMHG", "mmHg", "mmHg", "no conversion"],
        ["DIABP", "mm hg", "mmHg", "mmHg", "no conversion"],
        ["PULSE", "beats/min", "beats/min", "beats/min", "no conversion"],
        ["PULSE", "bpm", "beats/min", "beats/min", "no conversion"],
        ["PULSE", "BEATS/MIN", "beats/min", "beats/min", "no conversion"],
        ["PULSE", "per min", "beats/min", "beats/min", "no conversion"],
        ["TEMP", "C", "C", "C", "no conversion"],
        ["TEMP", "CELSIUS", "C", "C", "no conversion"],
        ["TEMP", "F", "F", "C", "F_to_C"],
        ["TEMP", "FAHRENHEIT", "F", "C", "F_to_C"],
        ["TEMP", "K", "K", "C", "K_to_C"],
        ["WEIGHT", "kg", "kg", "kg", "no conversion"],
        ["WEIGHT", "KG", "kg", "kg", "no conversion"],
        ["WEIGHT", "kilograms", "kg", "kg", "no conversion"],
        ["WEIGHT", "lb", "lb", "kg", "lb_to_kg"],
        ["WEIGHT", "LBS", "lb", "kg", "lb_to_kg"],
        ["HEIGHT", "cm", "cm", "cm", "no conversion"],
        ["HEIGHT", "CM", "cm", "cm", "no conversion"],
        ["HEIGHT", "in", "in", "cm", "in_to_cm"],
        ["HEIGHT", "INCH", "in", "cm", "in_to_cm"],
    ], columns=["vstestcd", "raw_unit", "vsorresu_normalized", "vsstresu_standard", "conversion_rule"])

def build_conversion_rules():
    return pd.DataFrame([
        ["TEMP", "F_to_C", "(F - 32) * 5 / 9", "1 decimal place"],
        ["TEMP", "K_to_C", "K - 273.15", "1 decimal place"],
        ["WEIGHT", "lb_to_kg", "lb * 0.45359237", "1 decimal place"],
        ["HEIGHT", "in_to_cm", "in * 2.54", "1 decimal place"],
    ], columns=["vstestcd", "conversion_rule", "formula", "rounding"])

def build_visit_map():
    return pd.DataFrame([
        ["SCREENING", "10"],
        ["BASELINE", "20"],
        ["WEEK 4", "30"],
        ["WEEK 8", "40"],
    ], columns=["visit_name", "visitnum"])

def build_implausibility():
    return pd.DataFrame([
        ["SYSBP", "mmHg", 70, 220],
        ["DIABP", "mmHg", 40, 140],
        ["PULSE", "beats/min", 30, 220],
        ["TEMP", "C", 34, 43],
        ["WEIGHT", "kg", 20, 300],
        ["HEIGHT", "cm", 80, 250],
    ], columns=["vstestcd", "standard_unit", "min_value", "max_value"])

def build_position_map():
    return pd.DataFrame([
        ["SITTING", "SITTING"],
        ["SUPINE", "SUPINE"],
        ["STANDING", "STANDING"],
    ], columns=["raw_position", "vspos"])

def build_fasting_map():
    return pd.DataFrame([
        ["Y", "Y"],
        ["N", "N"],
    ], columns=["raw_fasting", "vsfast"])

def main():
    OUTDIR.mkdir(exist_ok=True)
    main_spec = build_main_spec()
    test_map = build_test_map()
    unit_map = build_unit_map()
    conv = build_conversion_rules()
    visit_map = build_visit_map()
    impl = build_implausibility()
    pos = build_position_map()
    fast = build_fasting_map()

    main_spec.to_csv(OUTDIR / "vs_mapping_spec_validated_v3.csv", index=False)
    test_map.to_csv(OUTDIR / "vs_test_map_v3.csv", index=False)
    unit_map.to_csv(OUTDIR / "vs_unit_map_v3.csv", index=False)
    conv.to_csv(OUTDIR / "vs_conversion_rules_v3.csv", index=False)
    visit_map.to_csv(OUTDIR / "vs_visit_map_v3.csv", index=False)
    impl.to_csv(OUTDIR / "vs_implausibility_thresholds_v3.csv", index=False)
    pos.to_csv(OUTDIR / "vs_position_map_v3.csv", index=False)
    fast.to_csv(OUTDIR / "vs_fasting_map_v3.csv", index=False)

    readme = (
        "VS spec v3 package\n"
        "- Main spec: one row per target variable\n"
        "- Test map: raw test name to VSTESTCD/VSTEST\n"
        "- Unit map: raw units to normalized original units and standard units\n"
        "- Conversion rules: deterministic numeric conversion formulas\n"
        "- Visit map: VISIT_NAME to VISITNUM\n"
        "- Implausibility thresholds: post-standardization QC bounds\n"
        "- Position map: raw POSITION to VSPOS\n"
        "- Fasting map: raw FASTING to VSFAST\n"
    )
    (OUTDIR / "README.txt").write_text(readme, encoding="utf-8")
    print(f"Created outputs in: {OUTDIR}")

if __name__ == "__main__":
    main()
