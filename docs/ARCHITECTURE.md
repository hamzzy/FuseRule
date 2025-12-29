# Architecture: The FuseRule Primitive

FuseRule is designed as an **Infrastructure Primitive**. Unlike product-focused rule engines that bundle complex UIs and management layers, FuseRule focuses on being a robust, composable "Hard Core" for rule evaluation.

## 🏗️ Core Philosophy: Hard Core, Soft Edges

We believe a rule engine should protect its internal semantics while being agnostic to its environment.

### 1. The Hard Core (Semantic Engine)
The core engine manages:
- **Batch Processing Loop**: Coordinating Arrow data through the evaluator.
- **Windowing**: Managing temporal data buffers for time-based rules.
- **State Transitions**: Deterministically tracking if a rule has moved from `False` to `True` (Activation).
- **Observability**: Emitting structured `EvaluationTrace` objects.

### 2. The Soft Edges (Pluggable Traits)
Everything else is an "Edge" defined by a Rust Trait:
- **`StateStore`**: Sled (persistence) or Memory (testing).
- **`RuleEvaluator`**: DataFusion (SQL) or any other engine.
- **`Agent`**: Webhooks, Loggers, Slack, or Kafka.

## 🚀 Technical Stack
- **Apache Arrow**: In-memory columnar data format for high-speed processing.
- **DataFusion**: Query execution engine for compiling SQL predicates into physical plans.
- **Sled**: Embedded KV store used for the default `StateStore` implementation.
- **Axum**: High-performance async HTTP framework.

## 🛰️ Audit Traces
Every ingestion yields a list of `EvaluationTrace` objects. This fulfills the **Observable Semantics** principle—users should never have to guess why a rule fired.

```json
{
  "rule_id": "r1",
  "rule_version": 2,
  "result": "True",
  "transition": "Activated",
  "agent_status": "success"
}
```

## 🔄 Live Reloading
By listening for `SIGHUP`, FuseRule can reload its configuration YAML, re-compile rules, and re-initialize agents without dropping the ingestion stream.
