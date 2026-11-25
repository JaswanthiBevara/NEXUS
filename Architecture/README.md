# NEXUS – System Architecture

## 1. Overview

PROJECT NEXUS is a data engineering platform that integrates batch processing and real-time streaming to process, store, and analyze order data.

The architecture consists of two major data processing pipelines:

- **Batch Pipeline:** Uses Azure Data Lake Storage Gen2, Azure Databricks, Apache Spark, and Delta Lake.
- **Real-Time Streaming Pipeline:** Uses Scala, Apache Kafka, Spark Structured Streaming, Apache Iceberg, and Kubernetes.

## 2. High-Level Architecture

### Batch Processing

1. Raw datasets are stored in Azure Data Lake Storage Gen2.
2. Azure Databricks processes the datasets using Apache Spark.
3. Data transformations support incremental loading and schema evolution.
4. Processed data is managed using Delta Lake.

### Real-Time Streaming

1. The Scala-based producer generates order events.
2. Events are published to an Apache Kafka topic.
3. Spark Structured Streaming consumes and processes Kafka events.
4. Events are validated and categorized.
5. Processed data is stored in Apache Iceberg tables.
6. The streaming applications run in Kubernetes.

## 3. Streaming Data Flow

```text
Order Event Producer (Scala)
          |
          v
     Apache Kafka
          |
          v
Spark Structured Streaming
          |
          v
     Event Processing
          |
     +----+----+
     |         |
     v         v
  Valid      Rejected
  Events      Events
     |
     v
Apache Iceberg
     |
     v
Curated Data
     |
     v
Business Analytics
```

## 4. Iceberg Table Design

The streaming pipeline uses the following Iceberg tables:

| Table | Purpose |
|---|---|
| raw_order_events | Stores incoming raw events |
| curated_order_events | Stores validated and curated events |
| rejected_order_events | Stores rejected events |
| late_order_events | Stores late-arriving events |
| watermark_state | Maintains watermark-related state |

Catalog: `nexus_catalog`

Namespace: `streaming`

## 5. Deployment Architecture

- Docker packages the producer, Iceberg consumer, and analytics applications.
- Kubernetes manages application deployments and supporting storage resources.
- Kafka provides the event streaming infrastructure.
- Persistent Volumes and Persistent Volume Claims support required data and state storage.

## 6. Business Analytics

The separate analytics application reads the curated Iceberg table in read-only mode and generates:

1. Total Sales Revenue
2. Total Orders
3. Top 10 Best-Selling Products
4. Monthly Sales Trend
5. Top 10 Customers

## 7. Design Principles

- Separate batch and streaming workloads.
- Use scalable distributed processing.
- Keep analytics separate from the production streaming consumer.
- Preserve raw, curated, rejected, and late-event data separately.
- Use Iceberg table capabilities for analytical storage and data management.
- Deploy streaming components independently using containers and Kubernetes.