# NEXUS – Testing and Data Quality

## Overview

This directory is maintained for documenting the testing and validation activities of Project NEXUS.

Testing covers the batch-processing and real-time streaming components.

## 1. Batch Pipeline Validation

The batch-processing implementation uses Azure Databricks, Apache Spark and Delta Lake.

The validation areas include:

- Raw dataset processing
- Incremental batch loading
- Schema evolution
- Processed data validation
- Delta Lake table verification

## 2. Streaming Pipeline Validation

The streaming implementation uses Scala, Apache Kafka, Spark Structured Streaming and Apache Iceberg.

The validation areas include:

- Kafka event ingestion
- Spark Structured Streaming processing
- Event schema validation
- Event classification
- Rejected event handling
- Late-arriving event handling
- Watermark-related processing
- Iceberg table writes
- Curated event verification

## 3. Iceberg Table Validation

The streaming implementation maintains five Iceberg tables:

| Table | Validation Focus |
|---|---|
| `raw_order_events` | Raw event records |
| `curated_order_events` | Curated event records |
| `rejected_order_events` | Rejected records |
| `late_order_events` | Late-arriving records |
| `watermark_state` | Watermark-related state |

## 4. Analytics Validation

The separate analytics application reads the curated Iceberg table.

The analytical outputs cover:

1. Total Sales Revenue
2. Total Orders
3. Top 10 Best-Selling Products
4. Monthly Sales Trend
5. Top 10 Customers

The batch dashboard is maintained separately in Azure Databricks and uses Delta Lake data.

## 5. Evidence

The `evidence/` directory is reserved for actual testing evidence, such as:

- Databricks execution screenshots
- Spark application execution screenshots
- Kubernetes pod status screenshots
- Kafka and streaming processing output
- Iceberg table query results
- Data validation results

Only evidence from actual executions should be added.

## 6. Important Notes

- This README describes the testing scope and validation areas.
- It does not claim that every listed validation has been executed.
- Actual test results and execution evidence should be added when available.
- No fabricated test reports or screenshots should be included.