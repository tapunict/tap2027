"""Stream Avro pizza orders from Kafka into an Iceberg table stored in RustFS.

The "lake" catalog (Iceberg REST + S3FileIO -> RustFS) is configured in
spark-defaults.conf. Each micro-batch is one atomic Iceberg commit (snapshot).
"""

import json
from urllib.request import urlopen

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hex, lit, raise_error, substring, when
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions.partitioning import days


REGISTRY_URL = "http://registry:8081/subjects/pizza_orders-value/versions/latest"
TABLE = "lake.pizza.orders"
CHECKPOINT = "/opt/spark/work-dir/pizza-lakehouse/checkpoint"


def main():
    with urlopen(REGISTRY_URL, timeout=10) as response:
        schema_metadata = json.load(response)

    schema_id = schema_metadata["id"]
    schema = schema_metadata["schema"]
    expected_header = f"00{schema_id:08X}"
    print(f"Using pizza_orders-value schema ID {schema_id}", flush=True)

    spark = SparkSession.builder.appName("PizzaOrdersToIceberg").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
        .option("subscribe", "pizza_orders")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Confluent framing: magic byte 0x00 + 4-byte schema ID, then the Avro payload.
    valid_header = hex(substring(col("value"), 1, 5)) == lit(expected_header)
    avro_bytes = when(
        col("value").isNotNull() & valid_header,
        substring(col("value"), 6, 2147483647),
    ).otherwise(raise_error(lit(f"Expected Confluent Avro schema ID {schema_id}")))

    orders = records.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_avro(avro_bytes, schema).alias("order"),
    ).select("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "order.*")

    # Create the table once, with the stream's schema and a hidden daily partition.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.pizza")
    if not spark.catalog.tableExists(TABLE):
        (
            spark.createDataFrame([], orders.schema)
            .writeTo(TABLE)
            .using("iceberg")
            .partitionedBy(days(col("kafka_timestamp")))
            .tableProperty("write.format.default", "parquet")
            .create()
        )
        print(f"Created Iceberg table {TABLE}", flush=True)

    query = (
        orders.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .option("fanout-enabled", "true")
        .trigger(processingTime="10 seconds")
        .toTable(TABLE)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
