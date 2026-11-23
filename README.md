PROJECT NEXUS

End-to-End Data Engineering Platform

PROJECT NEXUS is an end-to-end data engineering project that demonstrates batch processing, incremental data loading, real-time event streaming, data transformation, cloud storage integration, and analytical reporting.

The project combines Azure Databricks, Azure Data Lake Storage Gen2, Apache Kafka, Apache Spark Structured Streaming, Apache Iceberg, Scala, Docker, and Kubernetes to demonstrate modern data engineering workflows.

1. Project Architecture

PROJECT NEXUS consists of two major pipelines:

Batch Data Engineering Pipeline – Processes raw datasets, performs data cleaning and transformation, supports incremental updates, and creates analytical data models.

Real-Time Streaming Pipeline – Produces order events, processes them through Kafka and Spark Structured Streaming, and stores the processed data in Apache Iceberg tables.

High-Level Architecture

                  PROJECT NEXUS
                        |
          +-------------+-------------+
          |                           |
     BATCH PIPELINE             STREAMING PIPELINE
          |                           |
   Source CSV Files              JSON Order Events
          |                           |
   Azure Data Lake              Scala Kafka Producer
   Storage Gen2                       |
          |                      Apache Kafka
   Azure Databricks                   |
          |                   Spark Structured
   Bronze Layer                     Streaming
          |                           |
   Silver Layer                 Event Processing
          |                           |
   Gold Layer                   Apache Iceberg
          |                           |
   Analytical Views            Raw / Curated /
          |                    Rejected / Late
   Reporting & Analysis               |
                               Separate Analytics

2. Batch Data Engineering Pipeline

The batch pipeline is implemented using Azure Databricks with Scala and Apache Spark.

Technologies

Azure Databricks

Azure Data Lake Storage Gen2

Apache Spark

Scala

Spark SQL

Delta Lake

Data Lake Organization

The batch pipeline follows a layered data architecture:

Layer

Purpose

Bronze

Stores incoming raw datasets

Silver

Contains cleaned and transformed data

Gold

Contains business-ready analytical data

Source Datasets

The pipeline processes the following datasets:

Customers

Products

Orders

Additional datasets are used for incremental loading and schema evolution.

Processing Workflow

Read raw CSV datasets from ADLS Gen2.

Perform schema validation and data type handling.

Apply data cleaning and transformation.

Identify invalid records and separate rejected data.

Store processed data in the appropriate data lake layers.

Build analytical dimensions and fact tables.

Create SQL views for business reporting.

Data Modeling

The Gold layer follows a star schema approach.

Dimension tables:

dim_customer

dim_product

dim_date

Fact table:

fact_order

The fact table connects the dimensions using surrogate keys and supports analytical queries.

Incremental Batch Loading

The project also implements incremental processing to handle incoming order changes.

The incremental workflow supports:

INSERT – New order records.

UPDATE – Changes to existing order records.

STALE – Records with older update timestamps.

INVALID – Records that fail validation.

The pipeline uses timestamps to identify stale updates and applies changes to the Gold fact data while preserving existing keys for updated records.

Schema Evolution

A separate schema evolution dataset demonstrates how the pipeline handles changes in incoming source data.

3. Real-Time Streaming Pipeline

The streaming pipeline demonstrates real-time order event processing using Scala, Apache Kafka, Spark Structured Streaming, and Apache Iceberg.

Technologies

Scala

Apache Kafka

Apache Spark Structured Streaming

Apache Iceberg

Docker

Kubernetes

SBT

Streaming Architecture

JSON Micro-Batch Files
          |
          v
Scala Kafka Producer
          |
          v
Apache Kafka
Topic: nexus-order-events
          |
          v
Spark Structured Streaming
          |
          v
Event Validation & Processing
          |
          +----------------------+
          |          |           |
          v          v           v
        Raw      Curated      Rejected
       Events     Events       Events
          |          |           |
          +----------+-----------+
                     |
                     v
              Apache Iceberg
                     |
                     v
             Late Event Handling

Event Production

A Scala-based producer reads order event data and publishes messages to the Kafka topic:

nexus-order-events

The Kafka topic is configured with three partitions.

Stream Processing

A Scala Spark Structured Streaming application consumes events from Kafka.

The processing pipeline handles:

Kafka event consumption.

Event parsing and schema validation.

Data quality checks.

Valid and invalid event separation.

Late-arriving event handling.

Watermark-based event-time processing.

Micro-batch execution.

Checkpoint-based recovery.

Apache Iceberg Storage

The processed streaming data is stored using Apache Iceberg.

The project maintains the following tables:

Table

Purpose

raw_order_events

Stores incoming order events

curated_order_events

Stores successfully processed events

rejected_order_events

Stores invalid or rejected events

late_order_events

Stores late-arriving events

watermark_state

Maintains watermark-related processing state

Checkpointing and Recovery

Spark Structured Streaming checkpointing is configured to maintain processing progress and support recovery.

This helps the application resume processing from its recorded progress after a restart.

4. Kubernetes Deployment

The streaming components are deployed and managed using Kubernetes.

Docker images are built for the producer, consumer, and analytics application.

Kubernetes resources manage the deployment and execution of these components.

The deployment includes:

Kafka producer deployment.

Spark streaming consumer deployment.

Iceberg storage configuration.

Kubernetes analytics job.

The analytics application runs as a separate Kubernetes Job, keeping analytical execution independent of the existing streaming consumer.

5. Streaming Analytics

A separate Scala analytics application, NexusAnalytics, reads the processed Iceberg data and generates business insights.

The analytics application runs independently and does not modify the existing streaming pipeline or its checkpoints.

Five Analytical Metrics

Total Sales Revenue – Calculates the total revenue from processed order data.

Total Orders – Calculates the number of processed orders.

Top 10 Products – Identifies the highest-performing products.

Monthly Sales Trend – Analyzes sales across months.

Top 10 Customers – Identifies customers based on their order value.

The analytics job is deployed separately using Kubernetes.

6. Repository Structure

NEXUS/
|
|-- batch/
|   |-- Batch_process.ipynb
|   |-- NEXUS_Incremental_Batch_Loading.ipynb
|   |-- data_dictionary.csv
|   |
|   |-- raw/
|   |   |-- customers/
|   |   |-- orders/
|   |   |-- products/
|   |
|   |-- incremental/
|   |   |-- orders_updates/
|   |
|   |-- schema_evolution/
|       |-- orders_v2/
|
|-- streaming/
|   |-- src/main/scala/
|   |   |-- analytics/
|   |   |   |-- NexusAnalytics.scala
|   |   |
|   |   |-- consumer/
|   |   |   |-- OrderEventIcebergConsumer.scala
|   |   |
|   |   |-- producer/
|   |       |-- OrderEventProducer.scala
|   |
|   |-- docker/
|   |   |-- Dockerfile.producer
|   |   |-- Dockerfile.consumer.iceberg
|   |   |-- Dockerfile.analytics
|   |
|   |-- k8s/
|   |   |-- producer-deployment.yaml
|   |   |-- consumer-iceberg.yaml
|   |   |-- iceberg-storage.yaml
|   |   |-- analytics-job.yaml
|   |
|   |-- micro_batches/
|   |-- reference/
|   |-- order_event.schema.json
|   |-- build.sbt
|
|-- .gitignore
|-- README.md

7. Setup and Execution

Batch Pipeline

Configure Azure Data Lake Storage Gen2 access in Azure Databricks.

Upload the batch notebooks to the Databricks workspace.

Configure the storage paths and required access credentials.

Execute Batch_process.ipynb to run the main batch pipeline.

Execute NEXUS_Incremental_Batch_Loading.ipynb to demonstrate incremental processing.

Streaming Pipeline

Configure Docker and Kubernetes.

Build the producer, streaming consumer, and analytics Docker images.

Deploy the Kafka producer and streaming consumer using Kubernetes manifests.

Publish order events through the Scala producer.

Monitor event processing and storage in Apache Iceberg.

Execute the separate analytics Kubernetes Job to generate analytical results.

Refer to the individual configuration files and source code for deployment-specific settings.

8. Key Engineering Concepts Demonstrated

Batch data processing using Apache Spark.

Medallion architecture.

Incremental data loading.

Insert and update handling.

Data validation and rejected-record management.

Schema evolution.

Star schema data modeling.

Kafka-based event streaming.

Spark Structured Streaming.

Event-time processing and watermarks.

Late-arriving event handling.

Checkpoint-based recovery.

Apache Iceberg table storage.

Docker containerization.

Kubernetes deployments and jobs.

Independent analytical processing.