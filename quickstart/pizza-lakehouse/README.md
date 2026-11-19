# Pizza lakehouse

Lakehouse version of `../pizza-kafka-spark`. Avro pizza orders go from Kafka Connect Datagen to Kafka (2 brokers), then Spark Structured Streaming (2 workers) writes them to an Apache Iceberg table. The table's metadata lives in an Iceberg REST catalog and its files are stored in RustFS (S3-compatible, Apache 2.0).

| Service | Role | Local port |
|---|---|---|
| `broker1`, `broker2` | Kafka 4.4 (broker1 is the only KRaft controller: dev setup, not HA) | — |
| `registry`, `connect` | Schema Registry, Kafka Connect + Datagen | 18081, 18083 |
| `rustfs` | RustFS 1.0.5 S3-compatible storage, bucket `lakehouse` | 19000 (S3 API), 19001 (console at `/rustfs/console/`) |
| `iceberg-rest` | Iceberg REST catalog (SQLite in the `catalog-data` volume) | 18181 |
| `spark-master`, `spark-worker1/2`, `spark` | Spark 4.1.4 standalone + streaming driver | 18080, 14040 |

Lab credentials: `tapadmin` / `tapsecret123`. Ports are bound to `127.0.0.1` only.

## Run

```bash
docker compose up -d --build --wait broker1 broker2 registry connect rustfs iceberg-rest
docker compose exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:9092 \
  --create --if-not-exists --topic pizza_orders --partitions 2 --replication-factor 2
curl --fail-with-body -X PUT -H 'Content-Type: application/json' \
  --data @connector.json http://localhost:18083/connectors/pizza-orders/config
docker compose --profile spark up -d spark
docker compose exec spark /opt/spark/bin/spark-sql -S --conf spark.ui.enabled=false \
  -e "SELECT count(*) FROM lake.pizza.orders" 2>/dev/null
```

Stop with `docker compose --profile spark down` (add `-v` only to delete all data).

## Notes

- **Why RustFS and not MinIO:** the MinIO community edition is archived, gets no security fixes, and has no official images any more. RustFS is maintained and Apache-2.0 licensed. The S3 API is the same, so Spark and the catalog only need the endpoint `http://rustfs:9000`.
- **Inspect objects:** `docker compose exec rustfs sh -c 'rc alias set local http://localhost:9000 tapadmin tapsecret123 && rc object list --recursive local/lakehouse'`. `rc` is the RustFS CLI, with `mc`-style commands.
- **Single drive:** this lab RustFS uses one drive with no parity; it logs a warning about this. That is fine for a demo, but not durable storage.
- **Versions:** Iceberg 1.12.0 ships a Spark runtime only up to Spark 4.1, so this demo uses `apache/spark:4.1.4` (the Parquet demo uses 4.3.0).
- **Catalog:** the REST catalog makes each micro-batch an atomic commit on object storage. The `hadoop` catalog used in the local NYC 311 example is not safe for concurrent writers on S3.
- **Checkpoint:** stored in the `spark-data` volume on the driver. The workers need no shared volume.
- **Catalog image:** `apache/iceberg-rest-fixture` is a reference/test server. For production, use a catalog such as Apache Polaris, Lakekeeper, Nessie, or Unity Catalog.
