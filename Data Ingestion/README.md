# NEXUS – Data Ingestion

## Overview

Project NEXUS implements two data ingestion pipelines:

1. Batch Data Ingestion
2. Real-Time Streaming Data Ingestion

Both pipelines use Apache Spark but follow different processing and storage approaches.

---

## 1. Batch Data Ingestion

**Technology:** Azure Databricks, ADLS Gen2, Apache Spark and Delta Lake.

The batch pipeline processes retail datasets using Azure Databricks notebooks and stores the processed data in Delta Lake tables.

### Notebooks

The batch implementation contains:

- `Batch_process.ipynb`
- `NEXUS_Incremental_Batch_Loading.ipynb`

### Dataset Organization

The batch datasets are maintained under the `Datasets` directory.

- Raw customer data
- Raw order data
- Raw product data
- Incremental order updates
- Schema evolution order data

### Key Processing Areas

- Batch data processing
- Incremental batch loading
- Delta Lake storage
- Schema evolution

### Location

`Data Ingestion/Batch/`

---

## 2. Real-Time Streaming Data Ingestion

**Technology:** Scala, Apache Spark Structured Streaming, Apache Kafka, Apache Iceberg and Kubernetes.

The streaming pipeline processes retail order events in real time.

### Pipeline Flow

1. The Scala producer generates order events.
2. Events are published to the Apache Kafka topic `nexus-order-events`.
3. The Scala Spark Structured Streaming consumer reads events from Kafka.
4. Events are processed and classified.
5. Processed records are stored in Apache Iceberg tables.
6. The separate analytics application reads curated events for business analysis.

### Main Components

**Producer**

`producer/OrderEventProducer.scala`

Generates and publishes order events to Kafka.

**Iceberg Consumer**

`consumer/OrderEventIcebergConsumer.scala`

The active streaming consumer processes Kafka events and writes the results to Iceberg.

**Analytics Application**

`analytics/NexusAnalytics.scala`

Reads curated Iceberg data and calculates five business metrics.

### Iceberg Tables

The streaming implementation uses the following tables under `nexus_catalog.streaming`:

1. `raw_order_events`
2. `curated_order_events`
3. `rejected_order_events`
4. `late_order_events`
5. `watermark_state`

### Additional Capabilities

- Kafka-based event ingestion
- Spark Structured Streaming processing
- Event validation and classification
- Late-event handling
- Watermark-based processing
- Apache Iceberg table storage
- Snapshot and time-travel support
- Row-level updates using `MERGE INTO`

### Reference Data

The streaming Spark project contains:

- `reference/customers.csv`
- `reference/products.csv`

These files provide reference data used by the consumer.

### Location

`Data Ingestion/Streaming/NEXUS_Spark_Project/`

---

## 3. Batch vs Streaming

| Feature | Batch | Streaming |
|---|---|---|
| Processing | Batch | Real-time event processing |
| Platform | Azure Databricks | Spark on Kubernetes |
| Language | Python notebooks | Scala |
| Processing Engine | Apache Spark | Spark Structured Streaming |
| Storage | Delta Lake | Apache Iceberg |
| Ingestion | Dataset-based | Kafka-based |

---

## 4. Project Structure

```text
Data Ingestion/
|
|-- Batch/
|   |-- Batch_process.ipynb
|   |-- NEXUS_Incremental_Batch_Loading.ipynb
|
|-- Streaming/
|   |-- NEXUS_Spark_Project/
|       |-- build.sbt
|       |-- reference/
|       |-- src/main/scala/
|           |-- producer/
|           |-- consumer/
|           |-- analytics/
```

---

## 5. Important Notes

- Batch processing and streaming ingestion are separate implementations.
- Batch processing uses Delta Lake.
- The active streaming consumer uses Apache Iceberg.
- `OrderEventConsumer.scala` is a legacy Delta consumer and is not the active Iceberg implementation.
- `OrderEventIcebergConsumer.scala` is the active streaming consumer.
- `NexusAnalytics.scala` is a separate analytics application, not the Kafka producer or streaming consumer.