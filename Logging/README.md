# NEXUS – Logging and Error Handling

## Overview

Project NEXUS uses Spark and Kubernetes runtime logs to help observe application execution and troubleshoot processing issues.

The streaming implementation also maintains separate Iceberg tables for rejected and late-arriving events.

## 1. Application Logs

The streaming applications run as containerized workloads in Kubernetes.

Logs can be accessed using Kubernetes commands.

### View Producer Logs

```bash
kubectl logs -n streaming deployment/nexus-order-producer
```

### View Iceberg Consumer Logs

```bash
kubectl logs -n streaming deployment/nexus-order-consumer-iceberg
```

### View Pod Logs

```bash
kubectl get pods -n streaming
```

```bash
kubectl logs -n streaming <pod-name>
```

These commands provide access to application output available through the container runtime.

## 2. Streaming Error and Event Handling

The active Iceberg consumer maintains separate tables for different event-processing outcomes.

| Iceberg Table | Purpose |
|---|---|
| `raw_order_events` | Stores raw order event records |
| `curated_order_events` | Stores curated order event records |
| `rejected_order_events` | Stores rejected event records |
| `late_order_events` | Stores late-arriving event records |
| `watermark_state` | Maintains watermark-related state |

These tables are maintained in:

`nexus_catalog.streaming`

## 3. Data Quality and Event Handling

The streaming pipeline includes event validation and classification.

Events that are rejected are maintained separately from curated events.

Late-arriving events are handled separately using the streaming watermark-related processing.

This separation helps distinguish successfully curated records from rejected and late-arriving records.

## 4. Monitoring and Troubleshooting

Kubernetes provides commands to inspect application execution.

### Check Pods

```bash
kubectl get pods -n streaming
```

### Check Deployments

```bash
kubectl get deployments -n streaming
```

### Inspect Consumer Logs

```bash
kubectl logs -n streaming deployment/nexus-order-consumer-iceberg
```

### Follow Consumer Logs

```bash
kubectl logs -f -n streaming deployment/nexus-order-consumer-iceberg
```

### Describe Consumer Pod

```bash
kubectl describe pod -n streaming <pod-name>
```

## 5. Important Notes

- Application logs are accessed through Kubernetes.
- Rejected and late-arriving events are maintained in separate Iceberg tables.
- The `watermark_state` table is used for watermark-related state.
- This directory documents the logging and troubleshooting approach.
- No separate centralized logging platform or monitoring dashboard is documented as implemented in this project.