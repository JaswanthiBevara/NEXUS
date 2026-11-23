# PROJECT NEXUS - Mini Training Data Pack (synthetic)

Quality problems are intentional. Customer/product ids link across all files.

## batch/  (ADLS Gen2 -> Databricks, Delta Lake)
raw/products/products.csv      27 rows   dup product, conflicting price, negative/zero price, missing category/name, casing, inactive
raw/customers/customers.csv    42 rows   dup customers, bad/blank emails, city casing/spaces, bad & future signup dates, missing name
raw/orders/orders.csv         161 rows   exact duplicates (5), re-sent newer versions (6), qty 0/neg/blank/text, orphan or blank
                                         customer/product ids, mixed status spellings, DD-MM-YYYY / invalid / future dates
incremental/orders_updates/orders_updates.csv   17 rows  for Delta MERGE INTO: 7 corrections, 1 stale version
                                         (older last_updated - must NOT overwrite), 1 invalid correction, 8 brand-new orders
schema_evolution/orders_v2/orders_v2.csv        12 rows  adds sales_channel + discount_amount (Delta schema evolution)
data_dictionary.csv
Note: orders include a last_updated column (not in the brief's field list) so duplicates/MERGE can pick the latest version.

Suggested Delta demo order: load orders.csv -> bronze/silver/rejected; MERGE orders_updates; append orders_v2 with
mergeSchema; DESCRIBE HISTORY + VERSION AS OF; OPTIMIZE / VACUUM.
Suggested ADLS layout: abfss://raw@<acct>.dfs.core.windows.net/{products,customers,orders,...}/

## streaming/  (Kafka -> Spark Structured Streaming -> Iceberg, on Kubernetes)
order_events.jsonl                62 events, arrival (ingest_time) order, 10:00-11:00 UTC 19-Nov-2026
order_events_kafka_keyed.txt      key|json lines (key = order_id, else event_id)
micro_batches/order_events_001..005.json   5 micro-batches (~13 events) = 5 Iceberg snapshots
order_event.schema.json, sample_event.json
Built-in issues: 5 duplicate event_id, 3 late events (arrive 21-25 min after event_time), 1 null order_id,
1 invalid event_type (ORDER_ERR), negative quantity, negative price, orphan customer, orphan product,
1 future event_time (clock skew). 3 ORDER_UPDATED/ORDER_CANCELLED events target historical orders (use for MERGE INTO).
Batch 5 adds sales_channel + discount_amount -> Iceberg schema evolution (ALTER TABLE ADD COLUMN).
ingest_time is an added field used for arrival order and the late-data policy.
