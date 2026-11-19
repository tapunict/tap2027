"""Batch app for loading NYC 311 Parquet data into a Delta table.

Run with Apache Spark 4.1.4 and Delta Lake 4.4.1's Spark 4.1 / Scala 2.13
runtime.
"""

from pyspark.sql import SparkSession

from table_format_benchmark import benchmark_query


INPUT_PATH = "/opt/spark/work-dir/input/nyc311.parquet"
TABLE_PATH = "/opt/spark/work-dir/input/delta/nyc311"


def main() -> None:
    spark = (
        SparkSession.builder.appName("NYC311DeltaDemo")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("WARN")
        (
            spark.read.parquet(INPUT_PATH)
            .write.format("delta")
            .mode("overwrite")
            .save(TABLE_PATH)
        )

        table = spark.read.format("delta").load(TABLE_PATH)
        borough_counts = table.groupBy("borough").count().orderBy(
            "count", ascending=False
        )
        benchmark_query(borough_counts, "delta")
        print(f"Rows in Delta table at {TABLE_PATH}: {table.count():,}")
        borough_counts.show(truncate=False)
        spark.sql(f"DESCRIBE HISTORY delta.`{TABLE_PATH}`").select(
            "version", "timestamp", "operation"
        ).orderBy("version").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
