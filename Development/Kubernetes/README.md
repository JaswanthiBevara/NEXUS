# NEXUS – Kubernetes Deployment

## Overview

Project NEXUS uses Kubernetes manifests to define and manage the containerized components of its streaming architecture.

The Kubernetes configuration files are maintained in this directory.

## 1. Main Application Deployments

### producer-deployment.yaml

Defines the Kubernetes deployment configuration for the Scala Kafka order-event producer.

**Component:** Order Event Producer

### consumer-iceberg.yaml

Defines the deployment configuration for the active Spark Structured Streaming Iceberg consumer.

**Component:** Iceberg Streaming Consumer

### analytics-job.yaml

Defines the Kubernetes job configuration for the separate Spark analytics application.

**Component:** NEXUS Analytics

---

## 2. Configuration Files

### consumer-config.yaml

Contains configuration used by the streaming consumer deployment.

### storage.yaml

Contains Kubernetes storage-related configuration.

### iceberg-storage.yaml

Contains storage configuration for the Iceberg implementation.

---

## 3. Supporting and Additional Manifests

The following manifests are maintained for supporting operations and additional project configurations:

| Manifest | Purpose |
|---|---|
| `cleanup-pod.yaml` | Cleanup-related pod configuration |
| `data-transfer-pod.yaml` | Data-transfer pod configuration |
| `delta-inspector.yaml` | Delta-related inspection configuration |
| `consumer-deployment.yaml` | Older consumer deployment configuration |

These files are maintained separately from the active Iceberg consumer deployment.

---

## 4. Deployment Commands

Run commands from the project repository root.

### Deploy Producer

```bash
kubectl apply -f "Development/Kubernetes/producer-deployment.yaml"
```

### Deploy Iceberg Consumer

```bash
kubectl apply -f "Development/Kubernetes/consumer-iceberg.yaml"
```

### Submit Analytics Job

```bash
kubectl apply -f "Development/Kubernetes/analytics-job.yaml"
```

### Check Deployments

```bash
kubectl get deployments -A
```

### Check Pods

```bash
kubectl get pods -A
```

### View Consumer Logs

```bash
kubectl logs -n streaming deployment/nexus-order-consumer-iceberg
```

These are operational reference commands. Run them when you intend to interact with the Kubernetes cluster.

---

## 5. Active and Legacy Components

### Active Streaming Components

- Kafka order-event producer
- Spark Structured Streaming Iceberg consumer
- Separate Spark analytics application
- Apache Iceberg storage configuration

### Legacy Configurations

- Older Delta consumer deployment
- Delta inspection configuration

The legacy configurations are retained for reference and should not be confused with the active Iceberg pipeline.

---

## 6. Important Notes

- Kubernetes manifests define deployment and configuration resources.
- The active streaming consumer is `consumer-iceberg.yaml`.
- The analytics application is maintained separately through `analytics-job.yaml`.
- Storage and consumer configurations are maintained in separate YAML files.
- The presence of a manifest does not necessarily mean its resource is currently deployed or running.
- Preserve existing deployment settings when making configuration changes.