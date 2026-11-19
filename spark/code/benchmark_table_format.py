"""Read-only NYC 311 query benchmark for the three table formats."""

import sys

from pyspark.sql import SparkSession

from table_format_benchmark import benchmark_query


ICEBERG_TABLE = "demo.course.nyc311"
ICEBERG_WAREHOUSE = "file:///opt/spark/work-dir/input/iceberg"
PARQUET_PATH = "/opt/spark/work-dir/input/nyc311.parquet"
DELTA_PATH = "/opt/spark/work-dir/input/delta/nyc311"
HUDI_PATH = "/opt/spark/work-dir/input/hudi/nyc311"


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in {
        "parquet",
        "iceberg",
        "delta",
        "hudi",
    }:
        raise ValueError("Choose exactly one format: parquet, iceberg, delta, or hudi")

    table_format = sys.argv[1]
    builder = SparkSession.builder.appName(
        f"NYC311{table_format.title()}QueryBenchmark"
    )
    if table_format == "iceberg":
        builder = (
            builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "hadoop")
            .config("spark.sql.catalog.demo.warehouse", ICEBERG_WAREHOUSE)
        )
    elif table_format == "delta":
        builder = (
            builder.config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    elif table_format == "hudi":
        builder = (
            builder.config(
                "spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            )
            .config(
                "spark.serializer",
                "org.apache.spark.serializer.KryoSerializer",
            )
            .config(
                "spark.kryo.registrator",
                "org.apache.spark.HoodieSparkKryoRegistrar",
            )
        )

    spark = builder.getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        if table_format == "iceberg":
            table = spark.table(ICEBERG_TABLE)
        elif table_format == "delta":
            table = spark.read.format("delta").load(DELTA_PATH)
        elif table_format == "parquet":
            table = spark.read.parquet(PARQUET_PATH)
        else:
            table = spark.read.format("hudi").load(HUDI_PATH)

        print(f"format={table_format} rows={table.count():,}")
        borough_counts = table.groupBy("borough").count().orderBy(
            "count", ascending=False
        )
        benchmark_query(borough_counts, table_format)
        borough_counts.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
