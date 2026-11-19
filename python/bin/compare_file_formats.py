"""Compare CSV, Avro, Parquet, ORC, and Arrow IPC encodings of NYC 311 data.

Run this script through the tap:python image; see the Data Persistence
notebook for the Docker build and run commands.
"""

from collections import Counter
from math import isclose
from pathlib import Path
from statistics import fmean
from shutil import copyfileobj
from urllib.parse import urlencode
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from fastavro import reader, writer
from pyarrow import orc


PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "data" / "formats-demo"
DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"

COLUMN_TYPES = {
    "unique_key": pa.string(),
    "created_date": pa.string(),
    "agency": pa.string(),
    "complaint_type": pa.string(),
    "borough": pa.string(),
    "latitude": pa.float64(),
    "longitude": pa.float64(),
}

AVRO_SCHEMA = {
    "type": "record",
    "name": "NYC311Request",
    "fields": [
        {"name": "unique_key", "type": "string"},
        {"name": "created_date", "type": ["null", "string"], "default": None},
        {"name": "agency", "type": ["null", "string"], "default": None},
        {"name": "complaint_type", "type": ["null", "string"], "default": None},
        {"name": "borough", "type": ["null", "string"], "default": None},
        {"name": "latitude", "type": ["null", "double"], "default": None},
        {"name": "longitude", "type": ["null", "double"], "default": None},
    ],
}


def download_csv(csv_path: Path) -> None:
    params = {
        "$select": ",".join(COLUMN_TYPES),
        "$where": (
            "created_date >= '2024-01-01T00:00:00' "
            "AND created_date < '2024-02-01T00:00:00'"
        ),
        "$order": "created_date DESC, unique_key DESC",
        "$limit": "50000",
    }
    url = f"{DATASET_URL}?{urlencode(params)}"
    with urlopen(url, timeout=120) as response, csv_path.open("wb") as output:
        copyfileobj(response, output)


def summarize(rows):
    row_count = 0
    borough_counts = Counter()
    latitudes = []

    for row in rows:
        row_count += 1
        if row["borough"] is not None:
            borough_counts[row["borough"]] += 1
        if row["latitude"] is not None:
            latitudes.append(row["latitude"])

    latitude_stats = (
        (min(latitudes), fmean(latitudes), max(latitudes))
        if latitudes
        else None
    )
    return row_count, borough_counts.most_common(5), latitude_stats


def report(label: str, path: Path, rows, expected_summary) -> None:
    summary = summarize(rows)
    row_count, borough_counts, latitude_stats = summary
    expected_count, expected_borough_counts, expected_latitude_stats = expected_summary

    if row_count != expected_count or borough_counts != expected_borough_counts:
        raise ValueError(f"{label} did not preserve the row count or borough counts")

    if latitude_stats != expected_latitude_stats:
        if latitude_stats is None or expected_latitude_stats is None:
            raise ValueError(f"{label} did not preserve latitude values")
        if (
            latitude_stats[0] != expected_latitude_stats[0]
            or not isclose(
                latitude_stats[1], expected_latitude_stats[1], rel_tol=1e-12
            )
            or latitude_stats[2] != expected_latitude_stats[2]
        ):
            raise ValueError(f"{label} did not preserve latitude statistics")

    print(f"{label:9} {path.stat().st_size:>12,} bytes  rows={row_count:,}")
    print(f"  borough top 5: {borough_counts}")
    print(f"  latitude min/mean/max: {latitude_stats}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / "nyc311.csv"
    avro_path = OUTPUT_DIR / "nyc311.avro"
    parquet_path = OUTPUT_DIR / "nyc311.parquet"
    orc_path = OUTPUT_DIR / "nyc311.orc"
    arrow_path = OUTPUT_DIR / "nyc311.arrow"

    download_csv(csv_path)
    table = pacsv.read_csv(
        csv_path,
        convert_options=pacsv.ConvertOptions(
            column_types=COLUMN_TYPES,
            strings_can_be_null=True,
        ),
    )
    records = table.to_pylist()
    expected_summary = summarize(records)

    with avro_path.open("wb") as stream:
        writer(stream, AVRO_SCHEMA, records, codec="null")

    pq.write_table(table, parquet_path, compression="NONE")
    orc.write_table(table, orc_path, compression="uncompressed")
    with pa.OSFile(str(arrow_path), "wb") as sink:
        with pa.ipc.new_file(sink, table.schema) as arrow_writer:
            arrow_writer.write_table(table)

    report("CSV", csv_path, records, expected_summary)
    with avro_path.open("rb") as stream:
        report("Avro", avro_path, reader(stream), expected_summary)
    report(
        "Parquet",
        parquet_path,
        pq.read_table(parquet_path).to_pylist(),
        expected_summary,
    )
    report(
        "ORC",
        orc_path,
        orc.read_table(orc_path).to_pylist(),
        expected_summary,
    )
    with pa.memory_map(str(arrow_path), "r") as source:
        arrow_table = pa.ipc.open_file(source).read_all()
        report("Arrow IPC", arrow_path, arrow_table.to_pylist(), expected_summary)

    print(f"\nFiles written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
