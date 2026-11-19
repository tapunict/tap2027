"""Batch app for loading NYC 311 Parquet data into a Hudi Copy-on-Write table."""

from pyspark.sql import SparkSession

from table_format_benchmark import benchmark_query


INPUT_PATH = "/opt/spark/work-dir/input/nyc311.parquet"
TABLE_PATH = "/opt/spark/work-dir/input/hudi/nyc311"
TABLE_NAME = "nyc311"
HUDI_OPTIONS = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.recordkey.field": "unique_key",
    "hoodie.datasource.write.precombine.field": "created_date",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.bulkinsert.shuffle.parallelism": "4",
    "hoodie.parquet.compression.codec": "snappy",
}


def main() -> None:
    spark = (
        SparkSession.builder.appName("NYC311HudiDemo")
        .config(
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
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("WARN")
        (
            spark.read.parquet(INPUT_PATH)
            .write.format("hudi")
            .options(**HUDI_OPTIONS)
            .mode("overwrite")
            .save(TABLE_PATH)
        )

        table = spark.read.format("hudi").load(TABLE_PATH)
        borough_counts = table.groupBy("borough").count().orderBy(
            "count", ascending=False
        )
        benchmark_query(borough_counts, "hudi")
        print(f"Rows in Hudi table at {TABLE_PATH}: {table.count():,}")
        borough_counts.show(truncate=False)
        table.select("_hoodie_commit_time").distinct().orderBy(
            "_hoodie_commit_time"
        ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
