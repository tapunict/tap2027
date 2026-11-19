"""Compare physical sizes of the NYC 311 file and table-format outputs."""

from pathlib import Path


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "formats-demo"
DATASETS = (
    ("Parquet input", DATA_DIR / "nyc311.parquet"),
    ("Iceberg", DATA_DIR / "iceberg" / "course" / "nyc311"),
    ("Delta", DATA_DIR / "delta" / "nyc311"),
    ("Hudi", DATA_DIR / "hudi" / "nyc311"),
)


def measure_path(path: Path) -> tuple[int, int, int]:
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = [child for child in path.rglob("*") if child.is_file()]
    else:
        raise FileNotFoundError(f"Expected generated data at {path}")

    if not files:
        raise ValueError(f"No files found under {path}")

    total_bytes = sum(file.stat().st_size for file in files)
    parquet_bytes = sum(
        file.stat().st_size for file in files if file.suffix.lower() == ".parquet"
    )
    return len(files), parquet_bytes, total_bytes - parquet_bytes


def main() -> None:
    measurements = [(name, *measure_path(path)) for name, path in DATASETS]
    parquet_input_bytes = measurements[0][2] + measurements[0][3]

    print(
        f"{'Dataset':<16} {'Files':>7} {'Parquet bytes':>16} "
        f"{'Other bytes':>16} {'Total bytes':>16} {'vs input':>11}"
    )
    for name, file_count, parquet_bytes, other_bytes in measurements:
        total_bytes = parquet_bytes + other_bytes
        difference_pct = (total_bytes / parquet_input_bytes - 1) * 100
        print(
            f"{name:<16} {file_count:>7,} {parquet_bytes:>16,} "
            f"{other_bytes:>16,} {total_bytes:>16,} {difference_pct:>+10.1f}%"
        )


if __name__ == "__main__":
    main()
