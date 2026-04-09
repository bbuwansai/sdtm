import json
from pathlib import Path
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom

DATASET_NAME = "DM"
DATASET_LABEL = "Demographics"
DATASET_STRUCTURE = "One record per subject"
DOMAIN_KEYS = "STUDYID, USUBJID"

def load_spec(base_dir: Path) -> pd.DataFrame:
    csv_path = base_dir / "dm_mapping_spec_validated_v5.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing validated spec: {csv_path}")
    return pd.read_csv(csv_path, dtype=str)

def prettify(elem: ET.Element) -> str:
    rough = ET.tostring(elem, encoding="utf-8")
    return minidom.parseString(rough).toprettyxml(indent="  ")

def build_define_xml(df: pd.DataFrame) -> ET.Element:
    root = ET.Element("Define", {
        "FileType": "Snapshot",
        "ODMVersion": "1.3.2",
        "DefineVersion": "2.1",
        "DraftFlag": "Yes"
    })

    study = ET.SubElement(root, "Study", {"OID": "STUDY.DM.DRAFT"})
    globals_el = ET.SubElement(study, "GlobalVariables")
    ET.SubElement(globals_el, "StudyName").text = "Draft DM Metadata from Spec"
    ET.SubElement(globals_el, "StudyDescription").text = "Draft Define-XML generated from validated DM mapping spec."
    ET.SubElement(globals_el, "ProtocolName").text = "TBD"

    mdv = ET.SubElement(study, "MetaDataVersion", {
        "OID": "MDV.DM.DRAFT",
        "Name": "Draft SDTM DM Metadata",
        "Description": "Generated from validated DM spec"
    })

    ig = ET.SubElement(mdv, "ItemGroupDef", {
        "OID": "IG.DM",
        "Name": DATASET_NAME,
        "Domain": DATASET_NAME,
        "Label": DATASET_LABEL,
        "Repeating": "No",
        "IsReferenceData": "No",
        "Purpose": "Tabulation",
        "Structure": DATASET_STRUCTURE,
        "DomainKeys": DOMAIN_KEYS
    })

    methods = []
    for _, row in df.iterrows():
        var = str(row.get("Target Variable", "") or row.get("target_variable", "")).strip()
        if not var:
            continue
        label = str(row.get("Target Label", "") or row.get("target_label", "") or var)
        vtype = str(row.get("Type", "") or row.get("target_type", "") or "Char")
        vlen = str(row.get("Length", "") or row.get("target_length", "") or "")
        core = str(row.get("Core", "") or row.get("core", "") or "")
        origin = str(row.get("Origin", "") or row.get("origin", "") or "")
        rule = str(row.get("Transformation / Derivation Rule", "") or row.get("rule", "") or "")
        fmt = str(row.get("Controlled Terms / Format", "") or row.get("controlled_terms_or_format", "") or "")
        traceability = str(row.get("Traceability Note", "") or row.get("traceability_note", "") or "")
        pnotes = str(row.get("Programming Notes", "") or row.get("programming_notes", "") or "")
        source_var = str(row.get("Source Variable", "") or row.get("source_variable", "") or "")

        item_oid = f"IT.DM.{var}"
        ET.SubElement(ig, "ItemRef", {
            "ItemOID": item_oid,
            "Mandatory": "Yes" if core == "Req" else "No",
            "OrderNumber": str(len(ig.findall('ItemRef')) + 1)
        })

        attrs = {
            "OID": item_oid,
            "Name": var,
            "Label": label,
            "DataType": "float" if vtype == "Num" else "text",
        }
        if vtype == "Char" and vlen:
            try:
                attrs["Length"] = str(int(float(vlen)))
            except Exception:
                attrs["Length"] = str(vlen)

        item_def = ET.SubElement(mdv, "ItemDef", attrs)
        ET.SubElement(item_def, "Description").text = label
        ET.SubElement(item_def, "Origin").text = origin if origin else "TBD"
        if fmt and fmt not in {"None.", "None", ""}:
            ET.SubElement(item_def, "Format").text = fmt

        comment_parts = []
        if source_var:
            comment_parts.append(f"Source={source_var}")
        if traceability and traceability not in {"None.", "None"}:
            comment_parts.append(f"Traceability={traceability}")
        if pnotes and pnotes not in {"None.", "None"}:
            comment_parts.append(f"ProgrammingNotes={pnotes}")
        if comment_parts:
            ET.SubElement(item_def, "Comment").text = " | ".join(comment_parts)

        if origin in {"Assigned", "Derived"} and rule:
            method_oid = f"MTH.DM.{var}"
            ET.SubElement(item_def, "MethodRef", {"MethodOID": method_oid})
            methods.append((method_oid, var, rule))

    for method_oid, var, rule in methods:
        m = ET.SubElement(mdv, "MethodDef", {
            "OID": method_oid,
            "Name": f"{var} Derivation",
            "Type": "Computation"
        })
        ET.SubElement(m, "Description").text = rule

    return root

def main():
    script_dir = Path(__file__).resolve().parent
    spec_dir = script_dir / "ai_dm_spec_outputs_v5"
    out_dir = script_dir / "define_dm_outputs_v5"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_spec(spec_dir)
    root = build_define_xml(df)
    xml_text = prettify(root)

    (out_dir / "define_dm_draft_v5.xml").write_text(xml_text, encoding="utf-8")

    methods = []
    for _, row in df.iterrows():
        origin = str(row.get("Origin", "") or "")
        if origin in {"Assigned", "Derived"}:
            methods.append({
                "Target Variable": row.get("Target Variable"),
                "Origin": origin,
                "Rule": row.get("Transformation / Derivation Rule"),
                "Source Variable": row.get("Source Variable"),
            })
    pd.DataFrame(methods).to_csv(out_dir / "define_dm_methods_v5.csv", index=False)

    (out_dir / "define_dm_notes_v5.txt").write_text(
        "Draft Define-XML guidance:\n"
        "- This is variable-level metadata, not row-level lineage.\n"
        "- Regenerate after approved spec changes.\n"
        "- Use validated spec as the source of truth for metadata generation.\n",
        encoding="utf-8"
    )
    print(f"Created outputs in: {out_dir}")

if __name__ == "__main__":
    main()
