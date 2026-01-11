//! Queryable trace history storage
//!
//! Persists evaluation traces to a time-series database (ClickHouse)
//! for historical analysis and debugging.
//!
//! Enables queries like:
//! - "Show all activations for rule X in the last hour"
//! - "Show all failed agent executions today"
//! - "Show evaluation latency p99 for rule Y"

use anyhow::Result;
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info};

use crate::state::PredicateResult;
use crate::EvaluationTrace;

/// Configuration for trace storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceStoreConfig {
    /// Enable trace persistence
    pub enabled: bool,
    /// ClickHouse connection URL
    pub clickhouse_url: String,
    /// Database name
    pub database: String,
    /// Table name for traces
    pub table: String,
    /// Batch size for inserts
    pub batch_size: usize,
    /// Flush interval in seconds
    pub flush_interval_seconds: u64,
}

impl Default for TraceStoreConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            clickhouse_url: "http://localhost:8123".to_string(),
            database: "fuse_rule".to_string(),
            table: "evaluation_traces".to_string(),
            batch_size: 1000,
            flush_interval_seconds: 10,
        }
    }
}

/// Stored trace record (flattened for ClickHouse)
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct StoredTrace {
    /// Timestamp when evaluation occurred
    pub timestamp: DateTime<Utc>,
    /// Rule ID
    pub rule_id: String,
    /// Rule name
    pub rule_name: String,
    /// Rule version
    pub rule_version: u32,
    /// Predicate result (true/false/error)
    pub result: String,
    /// State transition (None/Activated/Deactivated)
    pub transition: String,
    /// Whether action was fired
    pub action_fired: bool,
    /// Agent status message
    pub agent_status: String,
    /// Evaluation duration in milliseconds (if tracked)
    pub duration_ms: Option<f64>,
    /// Matched row count (if tracked)
    pub matched_rows: Option<u64>,
}

impl From<&EvaluationTrace> for StoredTrace {
    fn from(trace: &EvaluationTrace) -> Self {
        Self {
            timestamp: Utc::now(),
            rule_id: trace.rule_id.clone(),
            rule_name: trace.rule_name.clone(),
            rule_version: trace.rule_version,
            result: match trace.result {
                PredicateResult::True => "true".to_string(),
                PredicateResult::False => "false".to_string(),
            },
            transition: trace.transition.clone(),
            action_fired: trace.action_fired,
            agent_status: trace.agent_status.clone().unwrap_or_else(|| "none".to_string()),
            duration_ms: None,
            matched_rows: None,
        }
    }
}

/// Query parameters for trace retrieval
#[derive(Debug, Clone)]
pub struct TraceQuery {
    /// Filter by rule ID
    pub rule_id: Option<String>,
    /// Filter by rule name
    pub rule_name: Option<String>,
    /// Start time (inclusive)
    pub start_time: Option<DateTime<Utc>>,
    /// End time (inclusive)
    pub end_time: Option<DateTime<Utc>>,
    /// Only show activations
    pub activations_only: bool,
    /// Only show errors
    pub errors_only: bool,
    /// Limit number of results
    pub limit: usize,
    /// Offset for pagination
    pub offset: usize,
}

impl Default for TraceQuery {
    fn default() -> Self {
        Self {
            rule_id: None,
            rule_name: None,
            start_time: None,
            end_time: None,
            activations_only: false,
            errors_only: false,
            limit: 100,
            offset: 0,
        }
    }
}

/// Trace storage with ClickHouse backend
pub struct TraceStore {
    client: Arc<Client>,
    config: TraceStoreConfig,
}

impl TraceStore {
    /// Create a new trace store
    pub async fn new(config: TraceStoreConfig) -> Result<Self> {
        let client = Client::default()
            .with_url(&config.clickhouse_url)
            .with_database(&config.database);

        let store = Self {
            client: Arc::new(client),
            config,
        };

        // Ensure table exists
        store.create_table_if_not_exists().await?;

        info!(
            database = %store.config.database,
            table = %store.config.table,
            "Trace store initialized"
        );

        Ok(store)
    }

    /// Create the traces table if it doesn't exist
    async fn create_table_if_not_exists(&self) -> Result<()> {
        let create_table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {database}.{table} (
                timestamp DateTime64(3) CODEC(DoubleDelta, LZ4),
                rule_id String CODEC(ZSTD(1)),
                rule_name String CODEC(ZSTD(1)),
                rule_version UInt32,
                result String CODEC(ZSTD(1)),
                transition String CODEC(ZSTD(1)),
                action_fired UInt8,
                agent_status String CODEC(ZSTD(1)),
                duration_ms Nullable(Float64),
                matched_rows Nullable(UInt64)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(timestamp)
            ORDER BY (rule_id, timestamp)
            TTL timestamp + INTERVAL 90 DAY
            SETTINGS index_granularity = 8192
            "#,
            database = self.config.database,
            table = self.config.table
        );

        self.client.query(&create_table_sql).execute().await?;

        debug!("Trace table created or verified");
        Ok(())
    }

    /// Store a batch of traces
    pub async fn store_traces(&self, traces: &[EvaluationTrace]) -> Result<()> {
        if traces.is_empty() {
            return Ok(());
        }

        let stored_traces: Vec<StoredTrace> = traces.iter().map(|t| t.into()).collect();

        let mut insert = self.client.insert(&self.config.table)?;

        for trace in stored_traces {
            insert.write(&trace).await?;
        }

        insert.end().await?;

        debug!(count = traces.len(), "Stored evaluation traces");

        Ok(())
    }

    /// Query traces with filters
    pub async fn query(&self, query: TraceQuery) -> Result<Vec<StoredTrace>> {
        let mut sql = format!(
            "SELECT * FROM {}.{} WHERE 1=1",
            self.config.database, self.config.table
        );

        // Build WHERE clauses
        if let Some(rule_id) = &query.rule_id {
            sql.push_str(&format!(" AND rule_id = '{}'", rule_id));
        }

        if let Some(rule_name) = &query.rule_name {
            sql.push_str(&format!(" AND rule_name = '{}'", rule_name));
        }

        if let Some(start_time) = query.start_time {
            sql.push_str(&format!(
                " AND timestamp >= '{}'",
                start_time.format("%Y-%m-%d %H:%M:%S")
            ));
        }

        if let Some(end_time) = query.end_time {
            sql.push_str(&format!(
                " AND timestamp <= '{}'",
                end_time.format("%Y-%m-%d %H:%M:%S")
            ));
        }

        if query.activations_only {
            sql.push_str(" AND transition = 'Activated'");
        }

        if query.errors_only {
            sql.push_str(" AND result = 'error'");
        }

        sql.push_str(" ORDER BY timestamp DESC");
        sql.push_str(&format!(" LIMIT {} OFFSET {}", query.limit, query.offset));

        let traces = self.client.query(&sql).fetch_all::<StoredTrace>().await?;

        Ok(traces)
    }

    /// Get activation count for a rule in time range
    pub async fn count_activations(
        &self,
        rule_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<u64> {
        #[derive(Row, Deserialize)]
        struct CountResult {
            count: u64,
        }

        let sql = format!(
            r#"
            SELECT count() as count
            FROM {}.{}
            WHERE rule_id = '{}'
              AND transition = 'Activated'
              AND timestamp >= '{}'
              AND timestamp <= '{}'
            "#,
            self.config.database,
            self.config.table,
            rule_id,
            start_time.format("%Y-%m-%d %H:%M:%S"),
            end_time.format("%Y-%m-%d %H:%M:%S")
        );

        let result = self.client.query(&sql).fetch_one::<CountResult>().await?;

        Ok(result.count)
    }

    /// Get p50, p95, p99 latencies for a rule
    pub async fn get_latency_percentiles(&self, rule_id: &str) -> Result<(f64, f64, f64)> {
        #[derive(Row, Deserialize)]
        struct LatencyResult {
            p50: f64,
            p95: f64,
            p99: f64,
        }

        let sql = format!(
            r#"
            SELECT
                quantile(0.5)(duration_ms) as p50,
                quantile(0.95)(duration_ms) as p95,
                quantile(0.99)(duration_ms) as p99
            FROM {}.{}
            WHERE rule_id = '{}'
              AND duration_ms IS NOT NULL
            "#,
            self.config.database, self.config.table, rule_id
        );

        let result = self.client.query(&sql).fetch_one::<LatencyResult>().await?;

        Ok((result.p50, result.p95, result.p99))
    }

    /// Get error rate for a rule in time range
    pub async fn get_error_rate(
        &self,
        rule_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<f64> {
        #[derive(Row, Deserialize)]
        struct ErrorRateResult {
            error_rate: f64,
        }

        let sql = format!(
            r#"
            SELECT
                countIf(result = 'error') / count() as error_rate
            FROM {}.{}
            WHERE rule_id = '{}'
              AND timestamp >= '{}'
              AND timestamp <= '{}'
            "#,
            self.config.database,
            self.config.table,
            rule_id,
            start_time.format("%Y-%m-%d %H:%M:%S"),
            end_time.format("%Y-%m-%d %H:%M:%S")
        );

        let result = self
            .client
            .query(&sql)
            .fetch_one::<ErrorRateResult>()
            .await?;

        Ok(result.error_rate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::PredicateResult;

    #[test]
    fn test_trace_store_config_default() {
        let config = TraceStoreConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.database, "fuse_rule");
        assert_eq!(config.table, "evaluation_traces");
    }

    #[test]
    fn test_stored_trace_conversion() {
        let trace = EvaluationTrace {
            rule_id: "test_rule".to_string(),
            rule_name: "Test Rule".to_string(),
            rule_version: 1,
            result: PredicateResult::True,
            transition: "Activated".to_string(),
            action_fired: true,
            agent_status: Some("success".to_string()),
        };

        let stored: StoredTrace = (&trace).into();

        assert_eq!(stored.rule_id, "test_rule");
        assert_eq!(stored.result, "true");
        assert!(stored.action_fired);
    }

    #[test]
    fn test_trace_query_default() {
        let query = TraceQuery::default();
        assert_eq!(query.limit, 100);
        assert_eq!(query.offset, 0);
        assert!(!query.activations_only);
    }
}
