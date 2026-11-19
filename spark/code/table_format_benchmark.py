"""Shared query timing for the local table-format demos."""

from statistics import median
from time import perf_counter

from pyspark.sql import DataFrame


def benchmark_query(query: DataFrame, table_format: str) -> None:
    query.collect()
    samples = []
    for _ in range(5):
        started = perf_counter()
        query.collect()
        samples.append(perf_counter() - started)

    sample_values = ",".join(f"{sample:.6f}" for sample in samples)
    print(
        "QUERY_BENCHMARK "
        f"format={table_format} query=borough_count warmups=1 runs=5 "
        f"median_seconds={median(samples):.6f} "
        f"samples_seconds={sample_values}"
    )
