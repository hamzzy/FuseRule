# FuseRule ⚡

**FuseRule** is a high-performance, developer-first rule engine built for the cloud-native ecosystem. It leverages **Apache Arrow** and **DataFusion** to provide a lightning-fast, SQL-expressive core for real-time data auditing and event processing.

Designed as an **Infrastructure Primitive**, FuseRule decouples its deterministic core from pluggable "edges" like persistence, evaluation engines, and notification agents.

## 🚀 Features

-   **SQL-Powered Rules**: Write complex predicates using standard SQL expressions.
-   **Infrastructure-First**: Decoupled core with swappable traits for `StateStore`, `RuleEvaluator`, and `Agent`.
-   **Real-Time Observability**: Returns machine-readable `EvaluationTrace` logs for every ingestion.
-   **Zero-Downtime Reloading**: Hot-swap rules and agents via `SIGHUP` without restarting the daemon.
-   **Cloud-Native Metrics**: Built-in Prometheus-formatted telemetry.
-   **Stateful Transitions**: Built-in state management for `Activated` and `Deactivated` transitions.

## 📦 Quickstart

### 1. Run with Docker
```bash
docker run -p 3030:3030 hamzzy/fuserule
```

### 2. Ingest Data
```bash
curl -X POST http://localhost:3030/ingest \
     -H "Content-Type: application/json" \
     -d '{"amount": 5000, "cpu_usage": 50.0}'
```

### 3. See the Trace
You'll receive a detailed audit trail of which rules fired and why.

## 🛠️ Design Philosophy: "Hard Core, Soft Edges"

FuseRule is built on the philosophy that the core logic of a rule engine should be a "boring," deterministic primitive, while the integration points (Ingress, Persistence, Notifications) should be flexible and pluggable.

Read more in our [Architecture Guide](docs/ARCHITECTURE.md).

## 📊 Monitoring
FuseRule exposes Prometheus metrics at `/metrics`.

- `fuserule_batches_processed_total`: Total number of data batches ingested.
- `fuserule_activations_total`: Total number of rule activations.
- `fuserule_agent_failures_total`: Total number of failed agent actions.

## 📜 License
Apache-2.0
