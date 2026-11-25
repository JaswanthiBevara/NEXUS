# NEXUS – Docker Configuration

## Overview

Project NEXUS uses Docker to package the streaming producer, Apache Spark Iceberg consumer and analytics application into container images.

The Dockerfiles are maintained in this directory.

## 1. Dockerfiles

### Dockerfile.producer

**Purpose:** Containerizes the Scala Kafka order-event producer.

- Base image: Eclipse Temurin 17 JDK for the build stage.
- Runtime image: Eclipse Temurin 17 JRE.
- Copies the assembled application JAR.
- Runs the application as a non-root user (`10001`).

### Dockerfile.consumer.iceberg

**Purpose:** Containerizes the Spark Structured Streaming Iceberg consumer.

- Build stage: Eclipse Temurin 17 JDK.
- Runtime image: Apache Spark 3.5.9.
- Copies the assembled application JAR.
- Creates directories for streaming checkpoints and the Iceberg warehouse.
- Uses non-root user ID `185`.
- Entrypoint: Spark `spark-submit`.

### Dockerfile.analytics

**Purpose:** Containerizes the separate Spark analytics application.

- Runtime image: Apache Spark 3.5.9.
- Copies the assembled application JAR.
- Uses non-root user ID `185`.
- Entrypoint: Spark `spark-submit`.

### legacy/Dockerfile.consumer

**Purpose:** Retained Dockerfile for the legacy Delta consumer implementation.

This is not the active Iceberg consumer Dockerfile.

## 2. Application Build

The Dockerfiles expect the assembled JAR at:

`target/scala-2.12/nexus-streaming-assembly.jar`

Build the application from:

`Data Ingestion/Streaming/NEXUS_Spark_Project/`

```bash
sbt clean assembly
```

## 3. Docker Image Build Commands

Run the following commands from the repository root.

### Producer

```bash
docker build -f "Development/Docker/Dockerfile.producer" -t nexus-order-producer:1.1 "Data Ingestion/Streaming/NEXUS_Spark_Project"
```

### Iceberg Consumer

```bash
docker build -f "Development/Docker/Dockerfile.consumer.iceberg" -t nexus-order-consumer-iceberg:1.1 "Data Ingestion/Streaming/NEXUS_Spark_Project"
```

### Analytics Application

```bash
docker build -f "Development/Docker/Dockerfile.analytics" -t nexus-analytics:1.1 "Data Ingestion/Streaming/NEXUS_Spark_Project"
```

## 4. Container Execution

- The producer container starts the Scala Kafka producer application.
- The Iceberg consumer container uses Spark `spark-submit`.
- The analytics container uses Spark `spark-submit` to run the separate analytics application.

Actual runtime arguments and configuration are supplied through the corresponding deployment configuration.

## 5. Important Notes

- All three active application Dockerfiles use the same assembled SBT JAR.
- The Docker build context must be the streaming Spark project root.
- The producer uses Java 17.
- The consumer and analytics images use Apache Spark 3.5.9.
- The Iceberg consumer and analytics application run as non-root user ID `185`.
- The legacy Delta consumer Dockerfile is retained separately under `legacy/`.
- Docker image tags in the commands are examples for building the project images.