# NEXUS – Development and Deployment

## Overview

The Development directory contains the containerization and Kubernetes deployment configurations used by Project NEXUS.

It is organized into:

1. Docker
2. Kubernetes
3. Scripts

## 1. Docker

**Location:** `Development/Docker/`

Contains Dockerfiles used to build container images for the streaming producer, Iceberg consumer and analytics application.

### Dockerfiles

- `Dockerfile.producer` – Builds the Kafka order-event producer image.
- `Dockerfile.consumer.iceberg` – Builds the Spark Iceberg consumer image.
- `Dockerfile.analytics` – Builds the Spark analytics application image.
- `legacy/Dockerfile.consumer` – Legacy Delta consumer Dockerfile.

The Docker build context is the `NEXUS_Spark_Project` directory because the Dockerfiles copy the assembled application JAR from the SBT build output.

## 2. Kubernetes

**Location:** `Development/Kubernetes/`

Contains Kubernetes YAML manifests used for application deployment and supporting infrastructure.

### Main Deployment Components

- `producer-deployment.yaml` – Producer deployment.
- `consumer-iceberg.yaml` – Active Iceberg consumer deployment.
- `analytics-job.yaml` – Analytics application job.

### Supporting Configuration and Storage

- `consumer-config.yaml`
- `iceberg-storage.yaml`
- `storage.yaml`

### Additional and Legacy Manifests

- `consumer-deployment.yaml`
- `delta-inspector.yaml`
- `cleanup-pod.yaml`
- `data-transfer-pod.yaml`

These include supporting and legacy Delta-related configurations. They should not be confused with the active Iceberg consumer deployment.

## 3. Scripts

**Location:** `Development/Scripts/`

Reserved for development and deployment automation scripts.

No standalone scripts are currently included in this directory.

## 4. Build and Deployment Commands

### Build the Streaming Application

Run from:

`Data Ingestion/Streaming/NEXUS_Spark_Project/`

```bash
sbt clean assembly
```

### Build Producer Docker Image

Run from the repository root:

```bash
docker build -f "Development/Docker/Dockerfile.producer" -t nexus-order-producer:1.1 "Data Ingestion/Streaming/NEXUS_Spark_Project"
```

### Build Iceberg Consumer Docker Image

```bash
docker build -f "Development/Docker/Dockerfile.consumer.iceberg" -t nexus-order-consumer-iceberg:1.1 "Data Ingestion/Streaming/NEXUS_Spark_Project"
```

### Build Analytics Docker Image

```bash
docker build -f "Development/Docker/Dockerfile.analytics" -t nexus-analytics:1.1 "Data Ingestion/Streaming/NEXUS_Spark_Project"
```

### Apply Kubernetes Manifests

Run from the repository root:

```bash
kubectl apply -f "Development/Kubernetes/producer-deployment.yaml"
kubectl apply -f "Development/Kubernetes/consumer-iceberg.yaml"
kubectl apply -f "Development/Kubernetes/analytics-job.yaml"
```

These are reference commands; execute them only when deployment or redeployment is intended.

## 5. Important Notes

- Docker images are built using the SBT project's assembled JAR.
- Docker build context must be the streaming Spark project root.
- The Iceberg consumer is the active streaming consumer.
- The older Delta consumer configuration is retained as legacy material.
- Kubernetes manifests are configuration files; their presence does not mean every manifest is currently deployed or active.