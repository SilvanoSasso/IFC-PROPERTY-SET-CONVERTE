"""Generate Civil 3D IFC property mapping files from an Excel source."""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover - dependency guard for runtime clarity
    raise RuntimeError(
        "pandas is required to read Excel files. Install dependencies listed in requirements.txt."
    ) from exc

try:
    import openpyxl  # noqa: F401  # pragma: no cover - imported for side effects
except ImportError as exc:  # pragma: no cover - dependency guard for runtime clarity
    raise RuntimeError(
        "openpyxl is required to read Excel files. Install dependencies listed in requirements.txt."
    ) from exc

REQUIRED_COLUMNS: Sequence[str] = (
    "PSet",
    "Nome_IFC",
    "Nome_Civil",
    "Tipo_IFC",
    "Entita_IFC",
)

COLUMN_ALIASES: Mapping[str, str] = {
    "Entità_IFC": "Entita_IFC",
    "Entity_IFC": "Entita_IFC",
    "Civil_Name": "Nome_Civil",
    "IFC_Name": "Nome_IFC",
}

NAME_CORRECTIONS: Mapping[str, str] = {
    "CordhLength": "ChordLength",
}

ALLOWED_TYPES = {
    "IfcBoolean",
    "IfcIdentifier",
    "IfcInteger",
    "IfcLabel",
    "IfcLengthMeasure",
    "IfcLogical",
    "IfcPositiveLengthMeasure",
    "IfcRatioMeasure",
    "IfcReal",
    "IfcText",
}

CSV_HEADER = ("PSet", "IFCName", "IsActive", "Group", "CivilSource")

DEFAULT_SOURCE = "Civil 3D"
DEFAULT_SOURCE_REFERENCE = "UserDefined"
ENTITY_SPLIT_PATTERN = re.compile(r"[;,\n]+")


@dataclass(frozen=True)
class MappingRecord:
    """Normalised row extracted from the Excel workbook."""

    pset: str
    ifc_name: str
    civil_name: str
    ifc_type: str
    entities: tuple[str, ...]

    @classmethod
    def from_raw(cls, record: Mapping[str, object]) -> "MappingRecord":
        normalised: MutableMapping[str, str] = {}
        for column in REQUIRED_COLUMNS:
            value = record.get(column, "")
            if value is None:
                text_value = ""
            else:
                text_value = str(value).strip()
            normalised[column] = text_value

        if not normalised["PSet"]:
            raise ValueError("PSet column contains empty value.")

        if not normalised["Nome_IFC"]:
            raise ValueError(f"Nome_IFC is required for PSet '{normalised['PSet']}'.")

        if not normalised["Nome_Civil"]:
            raise ValueError(
                f"Nome_Civil is required for IFC property '{normalised['Nome_IFC']}' in PSet '{normalised['PSet']}'."
            )

        corrected_name = NAME_CORRECTIONS.get(normalised["Nome_IFC"], normalised["Nome_IFC"])
        normalised["Nome_IFC"] = corrected_name

        if normalised["Tipo_IFC"] not in ALLOWED_TYPES:
            raise ValueError(
                f"Tipo_IFC '{normalised['Tipo_IFC']}' is not an allowed IFC measure type for property '{corrected_name}'."
            )

        entities_text = normalised["Entita_IFC"]
        if not entities_text:
            raise ValueError(
                f"Entita_IFC must contain at least one entity for property '{corrected_name}' in PSet '{normalised['PSet']}'."
            )

        entities = tuple(
            entity.strip()
            for entity in ENTITY_SPLIT_PATTERN.split(entities_text.replace("/", ","))
            if entity.strip()
        )
        if not entities:
            raise ValueError(
                f"Entita_IFC must contain at least one entity for property '{corrected_name}' in PSet '{normalised['PSet']}'."
            )

        return cls(
            pset=normalised["PSet"],
            ifc_name=corrected_name,
            civil_name=normalised["Nome_Civil"],
            ifc_type=normalised["Tipo_IFC"],
            entities=entities,
        )


def load_source(path: Path, sheet: str | None = None) -> List[MappingRecord]:
    if not path.exists():
        raise FileNotFoundError(path)

    try:
        frame = pd.read_excel(path, sheet_name=sheet, dtype=str)
    except Exception as exc:  # pragma: no cover - conversion of pandas errors to runtime errors
        raise RuntimeError(f"Failed to read Excel workbook '{path}': {exc}") from exc

    frame = frame.rename(columns={alias: target for alias, target in COLUMN_ALIASES.items() if alias in frame.columns})
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(
            "Missing required columns in Excel source: " + ", ".join(missing)
        )

    frame = frame.fillna("")

    records: List[MappingRecord] = []
    seen_pairs: set[tuple[str, str]] = set()
    seen_civil: set[tuple[str, str]] = set()
    for raw in frame.to_dict(orient="records"):
        record = MappingRecord.from_raw(raw)
        key = (record.pset, record.ifc_name)
        if key in seen_pairs:
            raise ValueError(f"Duplicate IFC property '{record.ifc_name}' detected for PSet '{record.pset}'.")
        seen_pairs.add(key)

        civil_key = (record.pset, record.civil_name.lower())
        if civil_key in seen_civil:
            raise ValueError(
                f"Duplicate Civil 3D property name '{record.civil_name}' detected for PSet '{record.pset}'."
            )
        seen_civil.add(civil_key)

        records.append(record)

    return records


def build_property_sets(records: Sequence[MappingRecord]):
    csv_rows: List[Dict[str, str]] = []
    grouped_entities: Dict[str, set[str]] = defaultdict(set)
    grouped_records: Dict[str, List[MappingRecord]] = defaultdict(list)

    for record in records:
        grouped_records[record.pset].append(record)
        grouped_entities[record.pset].update(record.entities)

    property_sets: List[Dict[str, object]] = []
    for pset in sorted(grouped_records):
        payload = []
        for record in sorted(grouped_records[pset], key=lambda item: item.ifc_name):
            payload.append(
                {
                    "Name": record.ifc_name,
                    "CivilName": record.civil_name,
                    "IFCType": record.ifc_type,
                    "Source": DEFAULT_SOURCE_REFERENCE,
                    "IsExported": True,
                }
            )
            csv_rows.append(
                {
                    "PSet": pset,
                    "IFCName": record.ifc_name,
                    "IsActive": "TRUE",
                    "Group": "Property",
                    "CivilSource": record.civil_name,
                }
            )

        property_sets.append(
            {
                "Name": pset,
                "Source": DEFAULT_SOURCE,
                "PrimaryMeasureType": "IfcPropertySet",
                "ApplicableEntities": sorted(grouped_entities[pset]),
                "Properties": payload,
            }
        )

    return property_sets, csv_rows


def write_json(data: Dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def write_csv(rows: Iterable[Dict[str, str]], path: Path) -> None:
    materialised = list(rows)
    if not materialised:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in materialised:
            writer.writerow(row)


def generate_mapping(
    source_path: Path,
    output_dir: Path,
    config_dir: Path,
    *,
    sheet: str | None = None,
    include_optional: bool = True,
) -> Dict[str, Path]:
    records = load_source(source_path, sheet=sheet)
    property_sets, csv_rows = build_property_sets(records)

    timestamp = datetime.now(timezone.utc).isoformat()
    mapping_document = {
        "Metadata": {
            "GeneratedOn": timestamp,
            "Source": source_path.name,
            "RecordCount": len(records),
        },
        "PropertySetTemplates": property_sets,
    }

    json_output = output_dir / "mapping_validated.json"
    csv_output = output_dir / "mapping_validated.csv"

    write_json(mapping_document, json_output)
    write_csv(csv_rows, csv_output)

    produced: Dict[str, Path] = {
        "IfcInfraExportPropertyMapping.json": json_output,
        "IfcInfraExportPropertyMapping.csv": csv_output,
    }

    if include_optional:
        mapping_summary = {
            "MappingVersion": "1.0",
            "GeneratedOn": timestamp,
            "PropertySetFile": "IfcInfraExportPropertyMapping.json",
        }
        configuration = {
            "Export": {
                "IncludePropertySets": True,
                "PropertyMapping": "IfcInfraExportPropertyMapping.json",
                "GeneratedOn": timestamp,
            }
        }

        optional_mapping_path = output_dir / "IfcInfraExportMapping.json"
        optional_configuration_path = output_dir / "IfcInfraConfiguration.json"

        write_json(mapping_summary, optional_mapping_path)
        write_json(configuration, optional_configuration_path)

        produced.update(
            {
                "IfcInfraExportMapping.json": optional_mapping_path,
                "IfcInfraConfiguration.json": optional_configuration_path,
            }
        )

    config_dir.mkdir(parents=True, exist_ok=True)
    for filename, source in produced.items():
        target = config_dir / filename
        if source.resolve() == target.resolve():
            continue
        target.write_bytes(source.read_bytes())

    return produced


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("mapping/mapping_source.xlsx"),
        help="Path to the Excel source file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("mapping"),
        help="Directory where intermediate mapping files are written.",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=Path("IfcInfraExportConfiguration"),
        help="Directory that mirrors the Civil 3D configuration output.",
    )
    parser.add_argument(
        "--sheet",
        help="Optional Excel sheet name to read from. Defaults to the first sheet when omitted.",
    )
    parser.add_argument(
        "--skip-optional",
        action="store_true",
        help="Do not generate IfcInfraExportMapping.json and IfcInfraConfiguration.json.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the Excel source without writing any output files.",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    records = load_source(args.source, sheet=args.sheet)

    if args.validate_only:
        print(f"Validated {len(records)} records from {args.source}.")
        return 0

    generate_mapping(
        args.source,
        args.output,
        args.config_dir,
        sheet=args.sheet,
        include_optional=not args.skip_optional,
    )
    print(f"Generated mapping files for {len(records)} records.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
