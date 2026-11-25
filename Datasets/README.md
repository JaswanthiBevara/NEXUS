# NEXUS – Datasets

## Overview

This directory contains the datasets and event files used by the batch-processing and real-time streaming components of Project NEXUS.

The datasets are organized into raw, incremental, schema evolution and streaming data.

## 1. Batch Processing Datasets

### Raw Data

Location: `raw/`

Contains the initial datasets:

- `raw/customers/customers.csv`
- `raw/orders/orders.csv`
- `raw/products/products.csv`

These datasets are used by the batch-processing notebooks.

### Incremental Data

Location: `incremental/`

- `incremental/orders_updates.csv`

Contains order update records used for incremental batch loading.

### Schema Evolution Data

Location: `schema_evolution/`

- `schema_evolution/orders_v2.csv`

Contains the updated order dataset used in schema evolution processing.

### Data Dictionary

- `data_dictionary.csv`

Contains the project's data dictionary.

---

## 2. Streaming Datasets

Location: `streaming/`

Contains the event data and supporting files used by the Kafka and Spark Structured Streaming implementation.

### Event Schema

- `order_event.schema.json`

Defines the order event structure.

### Event Data

- `order_events.jsonl`
- `order_events_007.json`
- `order_events_008.json`
- `order_events_009.json`
- `order_events_010.json`
- `order_events_011.json`
- `order_events_kafka_keyed.txt`
- `replay-patch.json`

### Micro-Batch Input Files

Location: `streaming/micro_batches/`

Contains:

- `order_events_001.json`
- `order_events_002.json`
- `order_events_003.json`
- `order_events_004.json`
- `order_events_005.json`

These files organize sample event inputs into separate micro-batch datasets.

---

## 3. Streaming Reference Data

The streaming Spark project also maintains reference datasets inside its project directory:

`Data Ingestion/Streaming/NEXUS_Spark_Project/reference/`

- `customers.csv`
- `products.csv`

These files are kept alongside the Spark project because the consumer uses them as reference data.

---

## 4. Dataset Usage

| Dataset Category | Purpose |
|---|---|
| Raw | Initial batch-processing inputs |
| Incremental | Incremental order loading |
| Schema Evolution | Updated order schema processing |
| Streaming | Order event inputs |
| Micro-batches | Separate event input batches |
| Reference | Customer and product reference data |

## 5. Important Notes

- Batch datasets are used by the Azure Databricks notebooks.
- Streaming event files support the real-time ingestion implementation.
- Reference CSV files are maintained inside the streaming Spark project.
- These datasets are project inputs and are separate from the processed Delta Lake and Apache Iceberg tables.
- Do not store passwords, access keys, connection strings or other credentials in this directory.