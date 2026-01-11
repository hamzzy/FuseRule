# Production Observability Guide

FuseRule provides comprehensive observability features for production deployments:

1. **Distributed Tracing** - OpenTelemetry spans across ingestion → evaluation → agent execution
2. **Immutable Audit Logs** - Complete history of rule changes and state transitions
3. **Queryable Trace History** - ClickHouse-backed trace storage for operational insights

## Table of Contents

- [Distributed Tracing](#distributed-tracing)
- [Audit Logs](#audit-logs)
- [Trace History](#trace-history)
- [Configuration](#configuration)
- [Examples](#examples)

## Distributed Tracing

### Overview

FuseRule integrates with OpenTelemetry to provide end-to-end distributed tracing. Traces capture:

- Batch ingestion with batch size and source
- Rule evaluation per-rule with timing
- Agent execution with success/failure status
- State operations (read/write)

### Configuration

Enable tracing in your `fuse_rule_config.yaml`:

```yaml
observability:
  tracing:
    enabled: true
    otlp_endpoint: "http://localhost:4317"  # Jaeger, Tempo, etc.
    service_name: "fuse-rule-engine"
    sampling_ratio: 1.0  # 1.0 = 100%, 0.1 = 10%
```

### Viewing Traces

1. **Jaeger** (recommended for development):
   ```bash
   docker run -d --name jaeger \
     -p 16686:16686 \
     -p 4317:4317 \
     jaegertracing/all-in-one:latest
   ```

   Access UI at http://localhost:16686

2. **Grafana Tempo** (recommended for production):
   ```yaml
   # tempo.yaml
   server:
     http_listen_port: 3200

   distributor:
     receivers:
       otlp:
         protocols:
           grpc:
             endpoint: 0.0.0.0:4317
   ```

### Trace Structure

```
process_batch (SpanKind: Server)
├── batch.size: 100
├── source: "http"
└── duration: 45ms
    ├── evaluate_rule (SpanKind: Internal)
    │   ├── rule.id: "high_price_alert"
    │   ├── rule.name: "High Price Alert"
    │   ├── batch.size: 100
    │   └── duration: 12ms
    ├── state_update (SpanKind: Internal)
    │   ├── rule.id: "high_price_alert"
    │   └── duration: 2ms
    └── execute_agent (SpanKind: Client)
        ├── agent.name: "slack-webhook"
        ├── rule.id: "high_price_alert"
        └── duration: 28ms
```

## Audit Logs

### Overview

Audit logs capture ALL significant events in an immutable, append-only log:

- Rule lifecycle: created, updated, deleted, enabled, disabled
- Configuration reloads
- State transitions: activated, deactivated
- Agent executions: started, succeeded, failed
- Engine lifecycle: started, stopped

### Configuration

```yaml
observability:
  audit_log_enabled: true
```

Audit logs are stored in the same Sled database as rule state (under `audit_log` tree).

### Event Types

| Event Type | Description |
|------------|-------------|
| `RuleCreated` | New rule was added |
| `RuleUpdated` | Rule predicate or config changed |
| `RuleDeleted` | Rule was removed |
| `RuleEnabled` | Rule was enabled |
| `RuleDisabled` | Rule was disabled |
| `ConfigReloaded` | Configuration was reloaded (SIGHUP) |
| `RuleActivated` | Rule transitioned False → True |
| `RuleDeactivated` | Rule transitioned True → False |
| `AgentExecutionStarted` | Agent began executing |
| `AgentExecutionSucceeded` | Agent completed successfully |
| `AgentExecutionFailed` | Agent failed with error |
| `StateCleared` | Rule state was manually cleared |
| `EngineStarted` | FuseRule engine started |
| `EngineStopped` | FuseRule engine stopped gracefully |

### Querying Audit Logs

#### Programmatic Access

```rust
use fuse_rule_core::observability::{AuditLog, AuditEventType};
use chrono::{Duration, Utc};

// Query events for a specific rule
let events = audit_log
    .query_by_rule_id("high_price_alert", 100)
    .await?;

for event in events {
    println!("{}: {} - {}",
        event.timestamp,
        event.event_type,
        event.description
    );
}

// Query by time range
let start = Utc::now() - Duration::hours(24);
let end = Utc::now();
let events = audit_log
    .query_by_time_range(start, end, 1000)
    .await?;

// Query by event type
let activation_events = audit_log
    .query_by_type(AuditEventType::RuleActivated, 100)
    .await?;
```

#### Example Output

```
2026-01-11 10:23:45 UTC: RuleCreated - Rule 'High Price Alert' created
2026-01-11 10:24:12 UTC: RuleEnabled - Rule 'High Price Alert' enabled
2026-01-11 10:25:33 UTC: RuleActivated - Rule 'High Price Alert' activated
2026-01-11 10:28:17 UTC: AgentExecutionSucceeded - Agent 'slack-webhook' executed
2026-01-11 11:15:42 UTC: RuleUpdated - Rule 'High Price Alert' updated
2026-01-11 14:32:19 UTC: RuleDeactivated - Rule 'High Price Alert' deactivated
```

### Compliance & Post-Mortems

Audit logs are **immutable** and include:
- Timestamp (nanosecond precision)
- Event type and description
- Rule ID and name (if applicable)
- Actor (user/system)
- Metadata (structured JSON)

Perfect for:
- Security audits
- Compliance reporting (SOC 2, HIPAA, etc.)
- Post-incident analysis
- Change tracking

## Trace History

### Overview

Trace history persists ALL evaluation traces to ClickHouse for:
- Long-term trend analysis
- Debugging historical issues
- Operational dashboards
- SLA monitoring

### Configuration

```yaml
observability:
  trace_store:
    enabled: true
    clickhouse_url: "http://localhost:8123"
    database: "fuse_rule"
    table: "evaluation_traces"
```

### ClickHouse Setup

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS fuse_rule;

-- Table is auto-created by FuseRule with:
-- - 90-day TTL (automatic cleanup)
-- - Partitioned by day for fast queries
-- - Compressed with ZSTD + DoubleDelta
-- - Ordered by (rule_id, timestamp) for optimal queries
```

### Querying Traces

#### Simple Queries

```rust
use fuse_rule_core::observability::{TraceStore, TraceQuery};
use chrono::{Duration, Utc};

// Get activations for a rule in the last hour
let query = TraceQuery {
    rule_id: Some("high_price_alert".to_string()),
    start_time: Some(Utc::now() - Duration::hours(1)),
    end_time: Some(Utc::now()),
    activations_only: true,
    limit: 100,
    ..Default::default()
};

let traces = trace_store.query(query).await?;

println!("Found {} activations", traces.len());
```

#### Advanced Analytics

```rust
// Get activation count
let count = trace_store
    .count_activations(
        "high_price_alert",
        Utc::now() - Duration::days(7),
        Utc::now()
    )
    .await?;

println!("Activations last 7 days: {}", count);

// Get latency percentiles
let (p50, p95, p99) = trace_store
    .get_latency_percentiles("high_price_alert")
    .await?;

println!("Latency - p50: {}ms, p95: {}ms, p99: {}ms", p50, p95, p99);

// Get error rate
let error_rate = trace_store
    .get_error_rate(
        "high_price_alert",
        Utc::now() - Duration::hours(24),
        Utc::now()
    )
    .await?;

println!("Error rate last 24h: {:.2}%", error_rate * 100.0);
```

#### Direct ClickHouse Queries

```sql
-- Activation timeline (last 24 hours)
SELECT
    toStartOfHour(timestamp) as hour,
    count() as activations
FROM fuse_rule.evaluation_traces
WHERE rule_id = 'high_price_alert'
  AND transition = 'Activated'
  AND timestamp >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC;

-- Top activated rules today
SELECT
    rule_name,
    count() as activations,
    avg(duration_ms) as avg_duration_ms
FROM fuse_rule.evaluation_traces
WHERE transition = 'Activated'
  AND toDate(timestamp) = today()
GROUP BY rule_name
ORDER BY activations DESC
LIMIT 10;

-- Error rate by hour
SELECT
    toStartOfHour(timestamp) as hour,
    countIf(result = 'error') / count() * 100 as error_rate_pct
FROM fuse_rule.evaluation_traces
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC;
```

## Configuration

### Full Example

```yaml
engine:
  persistence_path: "fuserule_state"
  max_pending_batches: 1000
  agent_concurrency: 10

observability:
  # Enable audit logging (minimal overhead)
  audit_log_enabled: true

  # Distributed tracing (some overhead, use sampling in prod)
  tracing:
    enabled: true
    otlp_endpoint: "http://localhost:4317"
    service_name: "fuse-rule-engine"
    sampling_ratio: 0.1  # Sample 10% of requests in production

  # Trace persistence (requires ClickHouse)
  trace_store:
    enabled: true
    clickhouse_url: "http://clickhouse:8123"
    database: "fuse_rule"
    table: "evaluation_traces"

schema:
  - name: "price"
    data_type: "float64"
  - name: "symbol"
    data_type: "utf8"

rules:
  - id: "high_price_alert"
    name: "High Price Alert"
    predicate: "price > 1000"
    action: "slack-webhook"
    version: 1
    enabled: true

agents:
  - name: "slack-webhook"
    type: "webhook"
    url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

### Performance Considerations

| Feature | Overhead | Recommendation |
|---------|----------|----------------|
| Audit Logs | < 1ms per event | Always enable in production |
| Distributed Tracing | 2-5ms per batch | Use sampling (10-25%) in high-throughput prod |
| Trace Persistence | 1-3ms per batch | Enable for critical rules or debugging |

## Examples

### Example 1: Debug Rule Activation

**Problem**: "Why did rule X activate at 3 AM last Tuesday?"

```rust
use chrono::{TimeZone, Utc};
use fuse_rule_core::observability::TraceQuery;

let start = Utc.with_ymd_and_hms(2026, 1, 5, 3, 0, 0).unwrap();
let end = Utc.with_ymd_and_hms(2026, 1, 5, 4, 0, 0).unwrap();

let query = TraceQuery {
    rule_id: Some("suspicious_activity".to_string()),
    start_time: Some(start),
    end_time: Some(end),
    activations_only: true,
    limit: 100,
    ..Default::default()
};

let traces = trace_store.query(query).await?;

for trace in traces {
    println!("{}: {} matched {} rows",
        trace.timestamp,
        trace.rule_name,
        trace.matched_rows.unwrap_or(0)
    );
}
```

### Example 2: Compliance Audit

**Problem**: "Show all rule changes made by user 'alice' in December"

```rust
use chrono::{TimeZone, Utc};
use fuse_rule_core::observability::AuditEventType;

let start = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();
let end = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

let all_events = audit_log.query_by_time_range(start, end, 10000).await?;

let alice_changes: Vec<_> = all_events
    .into_iter()
    .filter(|e| e.actor.as_deref() == Some("alice"))
    .filter(|e| matches!(e.event_type,
        AuditEventType::RuleCreated |
        AuditEventType::RuleUpdated |
        AuditEventType::RuleDeleted
    ))
    .collect();

for event in alice_changes {
    println!("{}: {} - {}",
        event.timestamp.format("%Y-%m-%d %H:%M:%S"),
        event.event_type,
        event.description
    );
}
```

### Example 3: SLA Dashboard

**Problem**: "What's our p99 latency and error rate?"

```rust
use chrono::{Duration, Utc};

let rules = vec!["high_price_alert", "volume_spike", "anomaly_detector"];

for rule_id in rules {
    // Get latency percentiles
    let (p50, p95, p99) = trace_store
        .get_latency_percentiles(rule_id)
        .await?;

    // Get error rate (last 24h)
    let error_rate = trace_store
        .get_error_rate(
            rule_id,
            Utc::now() - Duration::hours(24),
            Utc::now()
        )
        .await?;

    println!("Rule: {}", rule_id);
    println!("  Latency - p50: {:.1}ms, p95: {:.1}ms, p99: {:.1}ms", p50, p95, p99);
    println!("  Error Rate (24h): {:.2}%", error_rate * 100.0);
    println!();
}
```

## Production Deployment Checklist

- [ ] Enable audit logs (always)
- [ ] Configure distributed tracing with OTLP exporter
- [ ] Set appropriate sampling ratio (10-25% for high throughput)
- [ ] Deploy ClickHouse for trace history
- [ ] Set up Grafana dashboards for trace visualization
- [ ] Configure alerts on error rates and latency
- [ ] Test trace query performance on production data volume
- [ ] Document runbook for common debugging scenarios

## Troubleshooting

### "Traces not appearing in Jaeger"

1. Verify OTLP endpoint is reachable:
   ```bash
   curl http://localhost:4317
   ```

2. Check sampling ratio (must be > 0)

3. Verify OpenTelemetry tracer is initialized:
   ```
   2026-01-11 10:23:45 INFO Distributed tracing initialized
   ```

### "ClickHouse queries are slow"

1. Check table partitions:
   ```sql
   SELECT partition, rows
   FROM system.parts
   WHERE table = 'evaluation_traces';
   ```

2. Verify TTL is cleaning old data:
   ```sql
   SELECT count() FROM fuse_rule.evaluation_traces;
   ```

3. Add indexes for custom queries:
   ```sql
   ALTER TABLE fuse_rule.evaluation_traces
   ADD INDEX idx_transition transition TYPE set(0) GRANULARITY 1;
   ```

### "Audit log query is slow"

Audit logs use Sled's B-tree structure. For large installations (>1M events):

1. Implement external audit log storage (PostgreSQL, S3)
2. Periodically archive old audit events
3. Use time-range queries instead of full scans

---

**Next Steps**: See [ARCHITECTURE.md](ARCHITECTURE.md) for deep dive into observability design
