import json
import os

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

topic = os.getenv("KAFKA_TOPIC", "group-chat-events")
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafkaServer:9092")
schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schemaRegistry:8081")
group_id = os.getenv("GROUP_ID", "schema-registry-demo")

schema_registry = SchemaRegistryClient({"url": schema_registry_url})
deserialize_value = AvroDeserializer(schema_registry)
consumer = Consumer({
    "bootstrap.servers": bootstrap_servers,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
})

consumer.subscribe([topic])
print(f"Consuming Avro messages from {topic}; press Ctrl+C to stop.")
try:
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())

        event = deserialize_value(
            message.value(),
            SerializationContext(message.topic(), MessageField.VALUE),
        )
        print(json.dumps(event, ensure_ascii=False))
except KeyboardInterrupt:
    print("Stopping consumer.")
finally:
    consumer.close()
