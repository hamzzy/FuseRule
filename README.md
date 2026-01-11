# FuseRule ⚡

[![Crates.io](https://img.shields.io/crates/v/arrow-rule-agent.svg)](https://crates.io/crates/arrow-rule-agent)
[![Documentation](https://docs.rs/arrow-rule-agent/badge.svg)](https://docs.rs/arrow-rule-agent)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**FuseRule** is a high-performance, developer-first rule engine built for the cloud-native ecosystem. It leverages **Apache Arrow** and **DataFusion** to provide a lightning-fast, SQL-expressive core for real-time data auditing and event processing.

Designed as an **Infrastructure Primitive**, FuseRule decouples its deterministic core from pluggable "edges" like persistence, evaluation engines, and notification agents.

## 🚀 Features

- ⚡ **Arrow-Native**: Zero-copy columnar data processing for maximum performance
- 🔍 **SQL-Powered Rules**: Write complex predicates using standard SQL expressions
- 🏗️ **Infrastructure-First**: Decoupled core with swappable traits for `StateStore`, `RuleEvaluator`, and `Agent`
- 📊 **Real-Time Observability**: Returns machine-readable `EvaluationTrace` logs for every ingestion
- 🔄 **Zero-Downtime Reloading**: Hot-swap rules and agents via `SIGHUP` without restarting the daemon
- 📈 **Cloud-Native Metrics**: Built-in Prometheus-formatted telemetry
- 🔔 **Stateful Transitions**: Built-in state management for `Activated` and `Deactivated` transitions
- 🪟 **Time Windows**: Sliding time windows for aggregate functions (AVG, COUNT, SUM)
- 🔐 **API Key Authentication**: Secure endpoints with configurable API keys
- 🚦 **Rate Limiting**: Built-in rate limiting for ingestion endpoints
- 📡 **Multiple Ingestion Sources**: HTTP, Kafka, and WebSocket support
- 🎨 **Action Templates**: Handlebars templating for custom webhook payloads
- 🐛 **Interactive Debugging**: REPL and step-through debugger for rule development

## 📦 Installation

### As a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
fuse-rule-core = { path = "core" }
fuse-rule-agents = { path = "agents" } # Optional: for built-in agents
```

### As a Binary

```bash
cargo install fuse-rule
```

## 🎯 Quickstart

### 1. Create a Configuration File

Create `fuse_rule_config.yaml`:

```yaml
engine:
  persistence_path: "fuserule_state"
  ingest_rate_limit: 1000  # requests per second
  api_keys:
    - "sk_live_abc123..."

schema:
  - name: "price"
    data_type: "float64"
  - name: "symbol"
    data_type: "utf8"
  - name: "volume"
    data_type: "int32"

agents:
  - name: "logger"
    type: "logger"
  - name: "slack-webhook"
    type: "webhook"
    url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    template: |
      {
        "text": "🚨 {{rule_name}} triggered!",
        "symbol": "{{matched_data.0.symbol}}",
        "price": "{{matched_data.0.price}}"
      }

rules:
  - id: "high_price_alert"
    name: "High Price Alert"
    predicate: "price > 1000"
    action: "slack-webhook"
    version: 1
    enabled: true
    state_ttl_seconds: 3600  # Expire state after 1 hour

  - id: "volume_spike"
    name: "Volume Spike"
    predicate: "AVG(volume) > 10000"
    action: "logger"
    window_seconds: 60  # 60-second sliding window
    version: 1
    enabled: true
```

### 2. Start the Server

```bash
fuserule run --config fuse_rule_config.yaml --port 3030
```

### 3. Ingest Data

```bash
curl -X POST http://localhost:3030/ingest \
     -H "Content-Type: application/json" \
     -H "X-API-Key: sk_live_abc123..." \
     -d '[{"price": 1500, "symbol": "AAPL", "volume": 5000}]'
```

### 4. Check Rule States

```bash
# Get all rule states
curl http://localhost:3030/api/v1/state \
     -H "X-API-Key: sk_live_abc123..."

# Get specific rule state
curl http://localhost:3030/api/v1/state/high_price_alert \
     -H "X-API-Key: sk_live_abc123..."
```

## 📚 Usage as a Library

### Basic Example

```rust
use arrow_rule_agent::{RuleEngine, config::FuseRuleConfig};
use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration
    let config = FuseRuleConfig::from_file("fuse_rule_config.yaml")?;
    
    // Create engine from config
    let mut engine = RuleEngine::from_config(config).await?;
    
    // Create a test batch
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("symbol", DataType::Utf8, true),
    ]);
    
    let price_array = Arc::new(Float64Array::from(vec![1500.0, 500.0]));
    let symbol_array = Arc::new(StringArray::from(vec!["AAPL", "GOOGL"]));
    
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![price_array, symbol_array],
    )?;
    
    // Process batch and get evaluation traces
    let traces = engine.process_batch(&batch).await?;
    
    for trace in traces {
        if trace.action_fired {
            println!("Rule '{}' activated!", trace.rule_name);
        }
    }
    
    Ok(())
}
```

### Programmatic Rule Management

```rust
use arrow_rule_agent::{RuleEngine, rule::Rule};

// Add a rule programmatically
let rule = Rule {
    id: "custom_rule".to_string(),
    name: "Custom Rule".to_string(),
    predicate: "price > 100 AND volume < 50".to_string(),
    action: "logger".to_string(),
    window_seconds: None,
    version: 1,
    enabled: true,
};

engine.add_rule(rule).await?;

// Update a rule
engine.update_rule("custom_rule", updated_rule).await?;

// Toggle rule
engine.toggle_rule("custom_rule", false).await?;
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      FuseRule Engine                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │   Ingest     │───▶│   Evaluate   │───▶│   Activate   │ │
│  │   Sources    │    │   Rules      │    │   Agents     │ │
│  └──────────────┘    └──────────────┘    └──────────────┘ │
│         │                   │                    │           │
│         │                   │                    │           │
│         ▼                   ▼                    ▼           │
│  ┌──────────────────────────────────────────────────────┐   │
│  │          Arrow RecordBatch (Zero-Copy)              │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │   State      │    │   Windows     │    │   Metrics    │ │
│  │   Store      │    │   Buffers     │    │   (Prom)     │ │
│  └──────────────┘    └──────────────┘    └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Design Philosophy: "Hard Core, Soft Edges"

FuseRule is built on the philosophy that the core logic of a rule engine should be a "boring," deterministic primitive, while the integration points (Ingress, Persistence, Notifications) should be flexible and pluggable.

**Core (Hard):**
- Rule evaluation logic
- State transitions
- Window management
- Metrics collection

**Edges (Soft):**
- `StateStore` trait (Sled, Redis, etc.)
- `RuleEvaluator` trait (DataFusion, custom SQL engines)
- `Agent` trait (Webhooks, Loggers, custom actions)
- Ingestion sources (HTTP, Kafka, WebSocket)

## 🔧 CLI Commands

### Run Server

```bash
fuserule run --config fuse_rule_config.yaml --port 3030
```

### Validate Rules

```bash
# Validate all rules in config
fuserule validate --config fuse_rule_config.yaml

# Validate specific predicate
fuserule validate --config fuse_rule_config.yaml --predicate "price > 100"
```

### Interactive REPL

```bash
fuserule repl --config fuse_rule_config.yaml
```

### Rule Debugger

```bash
fuserule debug --config fuse_rule_config.yaml
```

## 📡 API Endpoints

### Public Endpoints

- `GET /status` - Server status
- `GET /health` - Health check with engine stats
- `GET /metrics` - Prometheus metrics

### Protected Endpoints (Require `X-API-Key` header)

#### Rule Management

- `GET /rules` - List all rules
- `POST /api/v1/rules` - Create new rule
- `PUT /api/v1/rules/:id` - Update rule
- `PATCH /api/v1/rules/:id` - Partial update (e.g., enable/disable)
- `DELETE /api/v1/rules/:id` - Delete rule
- `POST /api/v1/rules/validate` - Validate rule predicate

#### State Management

- `GET /api/v1/state` - Get all rule states
- `GET /api/v1/state/:rule_id` - Get specific rule state

#### Data Ingestion

- `POST /ingest` - Ingest JSON data (rate-limited)

## 📊 Monitoring

FuseRule exposes Prometheus metrics at `/metrics`:

- `fuserule_batches_processed_total` - Total batches ingested
- `fuserule_activations_total` - Total rule activations
- `fuserule_agent_failures_total` - Total agent failures
- `fuserule_evaluation_duration_seconds` - Evaluation latency histogram
- `fuserule_rule_evaluations_total{rule_id}` - Per-rule evaluation count
- `fuserule_rule_activations_total{rule_id}` - Per-rule activation count

## 🔌 Ingestion Sources

### HTTP (Default)

```bash
curl -X POST http://localhost:3030/ingest \
     -H "Content-Type: application/json" \
     -d '[{"price": 150, "symbol": "AAPL"}]'
```

### Kafka

Configure in `fuse_rule_config.yaml`:

```yaml
sources:
  - type: "kafka"
    brokers: ["localhost:9092"]
    topic: "events"
    group_id: "fuserule"
    auto_commit: true
```

### WebSocket

Configure in `fuse_rule_config.yaml`:

```yaml
sources:
  - type: "websocket"
    bind: "0.0.0.0:3031"
    max_connections: 1000
```

Connect and send JSON:

```javascript
const ws = new WebSocket('ws://localhost:3031/ws');
ws.send(JSON.stringify([{"price": 150, "symbol": "AAPL"}]));
```

## 🎨 Action Templates

Use Handlebars templates for custom webhook payloads:

```yaml
agents:
  - name: "custom-webhook"
    type: "webhook"
    url: "https://api.example.com/webhook"
    template: |
      {
        "alert": "{{rule_name}}",
        "timestamp": "{{timestamp}}",
        "data": {{#each matched_data}}
          {
            "price": {{price}},
            "symbol": "{{symbol}}"
          }{{#unless @last}},{{/unless}}
        {{/each}},
        "count": {{count}}
      }
```

## 🧪 Testing

### Unit Tests

```bash
cargo test --test unit_test
```

### Integration Tests

```bash
cargo test --test integration_test
```

### Property-Based Tests

```bash
cargo test --test property_test
```

## 📖 Documentation

- [Architecture Guide](docs/ARCHITECTURE.md) - Deep dive into design
- [API Documentation](https://docs.rs/arrow-rule-agent) - Full API reference (when published)
- **Local API Documentation**: Generate and view locally with:
  ```bash
  cargo doc --no-deps --open
  ```
  This will build and open the documentation in your browser at `target/doc/arrow_rule_agent/index.html`
- [Examples](examples/) - Code examples

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📜 License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## 🙏 Acknowledgments

- Built on [Apache Arrow](https://arrow.apache.org/) for zero-copy data processing
- Powered by [DataFusion](https://github.com/apache/datafusion) for SQL evaluation
- Inspired by the "Hard Core, Soft Edges" philosophy

