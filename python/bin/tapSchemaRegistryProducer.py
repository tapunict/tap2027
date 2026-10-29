import json
import os
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    topic_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

topic = os.getenv("KAFKA_TOPIC", "group-chat-events")
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafkaServer:9092")
schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schemaRegistry:8081")
pause_seconds = float(os.getenv("PAUSE_SECONDS", "2"))

schema = json.dumps({
    "type": "record",
    "name": "ChatMessage",
    "fields": [
        {"name": "ts", "type": "long"},
        {"name": "id", "type": "string"},
        {"name": "message", "type": "string"},
    ],
})

schema_registry = SchemaRegistryClient({"url": schema_registry_url})
serialize_value = AvroSerializer(
    schema_registry,
    schema,
    to_dict=lambda value, _context: value,
    conf={"subject.name.strategy": topic_subject_name_strategy},
)
producer = Producer({"bootstrap.servers": bootstrap_servers})


def report_delivery(error, message):
    if error is not None:
        print(f"Delivery failed: {error}", file=sys.stderr)
        return
    print(
        f"Sent to {message.topic()} partition {message.partition()} "
        f"offset {message.offset()}"
    )


print(f"Producing Avro messages to {topic}; press Ctrl+C to stop.")
index = 0
try:
    while True:
        event = {
            "ts": int(time.time() * 1000),
            "id": str(index),
            "message": f"TAP lesson message {index}",
        }
        serialized_value = serialize_value(
            event,
            SerializationContext(topic, MessageField.VALUE),
        )
        producer.produce(
            topic,
            key=str(index),
            value=serialized_value,
            on_delivery=report_delivery,
        )
        producer.poll(0)
        index += 1
        time.sleep(pause_seconds)
except KeyboardInterrupt:
    print("Stopping producer.")
finally:
    remaining = producer.flush(10)
    if remaining:
        print(f"{remaining} message(s) remain undelivered.", file=sys.stderr)
