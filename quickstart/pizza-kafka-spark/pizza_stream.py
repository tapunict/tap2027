import json
from urllib.request import urlopen

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, hex, lit, raise_error, substring, when


REGISTRY_URL = "http://registry:8081/subjects/pizza_orders-value/versions/latest"
DATA_DIR = "/opt/spark/work-dir/pizza-demo"


def main():
    with urlopen(REGISTRY_URL, timeout=10) as response:
        schema_metadata = json.load(response)

    schema_id = schema_metadata["id"]
    schema = schema_metadata["schema"]
    expected_header = f"00{schema_id:08X}"
    print(f"Using pizza_orders-value schema ID {schema_id}", flush=True)

    spark = SparkSession.builder.appName("PizzaOrdersToParquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
        .option("subscribe", "pizza_orders")
        .option("startingOffsets", "earliest")
        .load()
    )

    valid_header = hex(substring(col("value"), 1, 5)) == lit(expected_header)
    avro_bytes = when(
        col("value").isNotNull() & valid_header,
        substring(col("value"), 6, 2147483647),
    ).otherwise(raise_error(lit(f"Expected Confluent Avro schema ID {schema_id}")))
    decoded = records.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_avro(avro_bytes, schema).alias("order"),
    ).select("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "order.*")

    query = (
        decoded.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"{DATA_DIR}/parquet")
        .option("checkpointLocation", f"{DATA_DIR}/checkpoint")
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
