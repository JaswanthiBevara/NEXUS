# NEXUS – Dashboards and Business Analytics

## Overview

Project NEXUS contains separate analytics implementations for batch processing and real-time streaming.

## 1. Batch Processing Dashboard

**Status:** Implemented and published

The batch-processing dashboard was created and published in Azure Databricks using Delta Lake tables.

### Dashboard Metrics

**Key Performance Indicators:**
- Total Orders
- Total Units Sold
- Total Customers
- Active Products

**Visualizations:**
- Order Distribution by Status
- Product Category Performance
- Monthly Order Trend
- Sales Channel Distribution
- Top 10 Customers by Quantity Ordered

### Technology Stack
- Platform: Azure Databricks
- Processing Engine: Apache Spark
- Storage Format: Delta Lake

### Dashboard Evidence

The published dashboard PDF is available at:

`evidence/NEXUS_Batch_Dashboard.pdf`

## 2. Streaming Business Analytics

**Status:** Analytics application implemented; dashboard not created.

The streaming analytics application is implemented in `NexusAnalytics.scala`.

It reads curated order events from the Apache Iceberg table:

`nexus_catalog.streaming.curated_order_events`

### Business Metrics

1. Total Sales Revenue
2. Total Orders
3. Top 10 Best-Selling Products
4. Monthly Sales Trend
5. Top 10 Customers

### Technology Stack
- Processing Engine: Apache Spark
- Language: Scala
- Storage Format: Apache Iceberg
- Catalog: `nexus_catalog`
- Namespace: `streaming`

The application calculates and displays analytical results. A separate visual dashboard for streaming analytics has not been created.

## 3. Streaming SQL Queries

The file `nexus-business-queries.sql` contains Spark SQL equivalents of the five metrics implemented in `NexusAnalytics.scala`.

It uses the curated Iceberg table and a temporary view to select the latest event for each order.

## 4. Batch vs Streaming Analytics

| Component | Batch | Streaming |
|---|---|---|
| Processing | Batch processing | Spark Structured Streaming |
| Storage | Delta Lake | Apache Iceberg |
| Platform | Azure Databricks | Spark on Kubernetes |
| Dashboard | Published | Not created |
| Analytics | Databricks Dashboard | Scala analytics application |

## 5. Important Notes

- Batch dashboard analytics use Delta Lake tables.
- Streaming analytics read the curated Iceberg table.
- The batch dashboard PDF is included as project evidence.
- Streaming SQL queries document the corresponding Scala analytics calculations.
- The two analytics implementations are separate components of Project NEXUS.