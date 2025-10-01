"""Generate Civil 3D IFC property mapping files from an Excel source."""
from __future__ import annotations

import argparse
import csv
import json
import logging
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Mapping, MutableMapping, Sequence

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
    "Name",
    "Source",
    "Type",
    "Entities",
)

COLUMN_ALIASES: Mapping[str, str] = {
    "Nome_IFC": "Name",
    "Parametro IFC": "Name",
    "IFC": "Name",
    "Nome_Civil": "Source",
    "Parametro Civil": "Source",
    "Parametro Civil 3D": "Source",
    "Tipo_IFC": "Type",
    "Tipo IFC": "Type",
    "Entita_IFC": "Entities",
    "Entità_IFC": "Entities",
    "Entità IFC": "Entities",
}

NAME_CORRECTIONS: Mapping[str, str] = {
    "CordhLength": "ChordLength",
    "Insulation Type": "InsulationType",
}

CSV_HEADER = ("PSet", "Name", "Attivo", "Gruppo", "Source")
DEFAULT_PRIMARY_TYPE = "IfcLabel"
ENTITY_SEPARATORS = (";", ",", "\n")


@dataclass(frozen=True)
class MappingRecord:
    """Normalised row extracted from the Excel workbook."""

    pset: str
    name: str
    source: str
    type: str
    entities: tuple[str, ...]

    @classmethod
    def from_raw(cls, record: Mapping[str, object]) -> "MappingRecord":
        normalised: MutableMapping[str, str] = {}
        for column in REQUIRED_COLUMNS:
            value = record.get(column, "")
            text_value = "" if value is None else str(value).strip()
            normalised[column] = text_value

        if not normalised["PSet"]:
            raise ValueError("PSet column contains empty value.")

        name = NAME_CORRECTIONS.get(normalised["Name"], normalised["Name"])
        source = NAME_CORRECTIONS.get(normalised["Source"], normalised["Source"])
        type_name = normalised["Type"] or DEFAULT_PRIMARY_TYPE

        if not name:
            raise ValueError(f"Missing IFC property name for PSet '{normalised['PSet']}'.")

        if not source:
            raise ValueError(f"Missing Civil 3D source for property '{name}' in PSet '{normalised['PSet']}'.")

        if not type_name:
            raise ValueError(f"Missing IFC type for property '{name}' in PSet '{normalised['PSet']}'.")

        entities_text = normalised["Entities"].replace("/", ";")
        if not entities_text:
            raise ValueError(
                f"Missing IFC entities for property '{name}' in PSet '{normalised['PSet']}'."
            )

        normalised_text = entities_text
        for separator in ENTITY_SEPARATORS[1:]:
            normalised_text = normalised_text.replace(separator, ENTITY_SEPARATORS[0])

        entities = tuple(part.strip() for part in normalised_text.split(ENTITY_SEPARATORS[0]) if part.strip())
        if not entities:
            raise ValueError(
                f"Missing IFC entities for property '{name}' in PSet '{normalised['PSet']}'."
            )

        return cls(
            pset=normalised["PSet"],
            name=name,
            source=source,
            type=type_name,
            entities=entities,
        )


def load_source(path: Path, *, sheet: str | None = None) -> List[MappingRecord]:
    if not path.exists():
        raise FileNotFoundError(path)

    try:
        frame = pd.read_excel(path, sheet_name=sheet, dtype=str)
    except Exception as exc:  # pragma: no cover - conversion of pandas errors to runtime errors
        raise RuntimeError(f"Failed to read Excel workbook '{path}': {exc}") from exc

    rename_map = {alias: target for alias, target in COLUMN_ALIASES.items() if alias in frame.columns}
    if rename_map:
        frame = frame.rename(columns=rename_map)

    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError("Missing required columns in Excel source: " + ", ".join(missing))

    frame = frame.fillna("")

    records: List[MappingRecord] = []
    seen_pairs: set[tuple[str, str]] = set()
    seen_sources: set[tuple[str, str]] = set()

    for row in frame.to_dict(orient="records"):
        if all(not str(value).strip() for value in row.values()):
            continue

        record = MappingRecord.from_raw(row)

        key = (record.pset, record.name.lower())
        if key in seen_pairs:
            raise ValueError(f"Duplicate IFC property '{record.name}' detected for PSet '{record.pset}'.")
        seen_pairs.add(key)

        source_key = (record.pset, record.source.lower())
        if source_key in seen_sources:
            raise ValueError(
                f"Duplicate Civil 3D source '{record.source}' detected for PSet '{record.pset}'."
            )
        seen_sources.add(source_key)

        records.append(record)

    if not records:
        raise ValueError("Excel source does not contain any valid mapping rows.")

    return records


def build_property_sets(records: Sequence[MappingRecord]):
    grouped: dict[str, list[MappingRecord]] = {}
    for record in records:
        grouped.setdefault(record.pset, []).append(record)

    property_sets: list[dict[str, object]] = []
    csv_rows: list[dict[str, str]] = []

    for pset in sorted(grouped):
        mappings = sorted(grouped[pset], key=lambda item: item.name.lower())
        entities = sorted({entity for record in mappings for entity in record.entities})

        property_templates = [
            {
                "Name": record.name,
                "Description": "",
                "PrimaryMeasureType": record.type,
                "Source": record.source,
            }
            for record in mappings
        ]

        csv_rows.extend(
            {
                "PSet": pset,
                "Name": record.name,
                "Attivo": "TRUE",
                "Gruppo": pset,
                "Source": record.source,
            }
            for record in mappings
        )

        property_sets.append(
            {
                "Name": pset,
                "ApplicableEntities": ";".join(entities),
                "TemplateType": "NOTDEFINED",
                "PropertyTemplates": property_templates,
            }
        )

    return property_sets, csv_rows


def write_json(data: Mapping[str, object] | list[object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    logging.info("Wrote JSON file: %s", path)


def write_csv(rows: Iterable[Mapping[str, str]], path: Path) -> None:
    materialised = list(rows)
    if not materialised:
        raise ValueError("No rows available to write CSV output.")

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in materialised:
            writer.writerow(row)
    logging.info("Wrote CSV file: %s", path)


def write_mapping_files(
    source_path: Path,
    output_dir: Path,
    config_dir: Path,
    *,
    property_sets: Sequence[Mapping[str, object]],
    csv_rows: Sequence[Mapping[str, str]],
) -> dict[str, Path]:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=timezone.utc).isoformat()
    json_payload = {
        "GeneratedOn": timestamp,
        "Source": source_path.name,
        "PropertySetTemplates": property_sets,
    }

    json_output = output_dir / "mapping_validated.json"
    csv_output = output_dir / "mapping_validated.csv"

    write_json(json_payload, json_output)
    write_csv(csv_rows, csv_output)

    produced = {
        "mapping_validated.json": json_output,
        "mapping_validated.csv": csv_output,
    }

    config_dir.mkdir(parents=True, exist_ok=True)
    for filename, source in produced.items():
        target = config_dir / filename
        if source.resolve() == target.resolve():
            continue
        shutil.copy2(source, target)
        logging.info("Copied %s -> %s", source, target)

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
        "--validate-only",
        action="store_true",
        help="Validate the Excel source without writing any output files.",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = parse_args(argv or sys.argv[1:])
    records = load_source(args.source, sheet=args.sheet)
    property_sets, csv_rows = build_property_sets(records)

    if args.validate_only:
        logging.info(
            "Validation succeeded for %s records from %s.",
            len(records),
            args.source,
        )
        print(f"Validated {len(records)} records from {args.source}.")
        return 0

    write_mapping_files(
        args.source,
        args.output,
        args.config_dir,
        property_sets=property_sets,
        csv_rows=csv_rows,
    )
    logging.info("Generated mapping files for %s records.", len(records))
    print(f"Generated mapping files for {len(records)} records.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
