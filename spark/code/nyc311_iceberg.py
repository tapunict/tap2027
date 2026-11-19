"""Batch app for loading NYC 311 data into an Iceberg table.

Run with Apache Spark 4.1.4 and Iceberg 1.12.0's Spark 4.1 / Scala 2.13 runtime.
"""

from pyspark.sql import SparkSession

from table_format_benchmark import benchmark_query


INPUT_PATH = "/opt/spark/work-dir/input/nyc311.parquet"
TABLE_NAME = "demo.course.nyc311"
WAREHOUSE_PATH = "file:///opt/spark/work-dir/input/iceberg"


def main() -> None:
    spark = (
        SparkSession.builder.appName("NYC311IcebergDemo")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "hadoop")
        .config("spark.sql.catalog.demo.warehouse", WAREHOUSE_PATH)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.course")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME}
            USING iceberg
            AS SELECT * FROM parquet.`{INPUT_PATH}`
            """
        )

        table = spark.table(TABLE_NAME)
        borough_counts = table.groupBy("borough").count().orderBy(
            "count", ascending=False
        )
        benchmark_query(borough_counts, "iceberg")
        print(f"Rows in {TABLE_NAME}: {table.count():,}")
        borough_counts.show(truncate=False)
        spark.sql(
            f"""
            SELECT committed_at, snapshot_id, operation
            FROM {TABLE_NAME}.snapshots
            ORDER BY committed_at
            """
        ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
