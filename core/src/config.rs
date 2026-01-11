use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct FuseRuleConfig {
    pub engine: EngineConfig,
    pub schema: Vec<FieldDef>,
    pub rules: Vec<RuleConfig>,
    pub agents: Vec<AgentConfig>,
    #[serde(default)]
    pub sources: Vec<SourceConfig>, // Data ingestion sources
    #[serde(default)]
    pub observability: ObservabilityConfig, // Tracing, audit logs, trace storage
}

#[derive(Debug, Deserialize, Clone)]
pub struct FieldDef {
    pub name: String,
    pub data_type: String, // "int32", "float64", "utf8", "bool"
}

#[derive(Debug, Deserialize, Clone)]
pub struct EngineConfig {
    pub persistence_path: String,
    #[serde(default = "default_max_pending_batches")]
    pub max_pending_batches: usize, // Backpressure: max batches queued before blocking
    #[serde(default = "default_agent_concurrency")]
    pub agent_concurrency: usize, // Number of concurrent agent workers
    #[serde(default)]
    pub ingest_rate_limit: Option<u32>, // Rate limit: requests per second (None = unlimited)
    #[serde(default)]
    pub api_keys: Vec<String>, // API keys from config file
}

fn default_max_pending_batches() -> usize {
    1000
}

fn default_agent_concurrency() -> usize {
    10
}

#[derive(Debug, Deserialize, Clone)]
pub struct RuleConfig {
    pub id: String,
    pub name: String,
    pub predicate: String,
    pub action: String,
    pub window_seconds: Option<u64>,
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub state_ttl_seconds: Option<u64>, // TTL for rule state (None = never expire)
    /// Optional description of what the rule does
    #[serde(default)]
    pub description: Option<String>,
    /// Optional tags for categorizing and filtering rules
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_enabled() -> bool {
    true
}

fn default_version() -> u32 {
    1
}

#[derive(Debug, Deserialize, Clone)]
pub struct AgentConfig {
    pub name: String,
    pub r#type: String, // "logger", "webhook", etc.
    pub url: Option<String>,
    pub template: Option<String>, // Handlebars template for webhook payloads
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "kafka")]
    Kafka {
        brokers: Vec<String>,
        topic: String,
        group_id: String,
        #[serde(default = "default_auto_commit")]
        auto_commit: bool,
    },
    #[serde(rename = "websocket")]
    WebSocket {
        bind: String,
        #[serde(default = "default_max_connections")]
        max_connections: usize,
    },
}

fn default_auto_commit() -> bool {
    true
}

fn default_max_connections() -> usize {
    1000
}

#[derive(Debug, Deserialize, Clone)]
pub struct ObservabilityConfig {
    /// Enable audit logging
    #[serde(default)]
    pub audit_log_enabled: bool,
    /// Distributed tracing configuration
    #[serde(default)]
    pub tracing: TracingConfig,
    /// Trace storage configuration
    #[serde(default)]
    pub trace_store: TraceStoreConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            audit_log_enabled: false,
            tracing: TracingConfig::default(),
            trace_store: TraceStoreConfig::default(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TracingConfig {
    /// Enable distributed tracing
    #[serde(default)]
    pub enabled: bool,
    /// OTLP endpoint (e.g., "http://localhost:4317")
    pub otlp_endpoint: Option<String>,
    /// Service name for traces
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Sampling ratio (0.0 to 1.0)
    #[serde(default = "default_sampling_ratio")]
    pub sampling_ratio: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: None,
            service_name: default_service_name(),
            sampling_ratio: default_sampling_ratio(),
        }
    }
}

fn default_service_name() -> String {
    "fuse-rule-engine".to_string()
}

fn default_sampling_ratio() -> f64 {
    1.0
}

#[derive(Debug, Deserialize, Clone)]
pub struct TraceStoreConfig {
    /// Enable trace persistence
    #[serde(default)]
    pub enabled: bool,
    /// ClickHouse connection URL
    #[serde(default = "default_clickhouse_url")]
    pub clickhouse_url: String,
    /// Database name
    #[serde(default = "default_database")]
    pub database: String,
    /// Table name for traces
    #[serde(default = "default_table")]
    pub table: String,
}

impl Default for TraceStoreConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            clickhouse_url: default_clickhouse_url(),
            database: default_database(),
            table: default_table(),
        }
    }
}

fn default_clickhouse_url() -> String {
    "http://localhost:8123".to_string()
}

fn default_database() -> String {
    "fuse_rule".to_string()
}

fn default_table() -> String {
    "evaluation_traces".to_string()
}

impl FuseRuleConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .build()?;

        let config: FuseRuleConfig = settings.try_deserialize()?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let mut agent_names = std::collections::HashSet::new();
        for agent in &self.agents {
            if !agent_names.insert(&agent.name) {
                anyhow::bail!("Duplicate agent name: {}", agent.name);
            }
            if agent.r#type == "webhook" && agent.url.is_none() {
                anyhow::bail!("Webhook agent '{}' missing URL", agent.name);
            }
        }

        let mut rule_ids = std::collections::HashSet::new();
        for rule in &self.rules {
            if !rule_ids.insert(&rule.id) {
                anyhow::bail!("Duplicate rule ID: {}", rule.id);
            }
            if !agent_names.contains(&rule.action) {
                anyhow::bail!(
                    "Rule '{}' references unknown agent '{}'",
                    rule.id,
                    rule.action
                );
            }
            if rule.predicate.trim().is_empty() {
                anyhow::bail!("Rule '{}' has an empty predicate", rule.id);
            }
        }

        if self.schema.is_empty() {
            anyhow::bail!("Configuration must define a schema");
        }

        Ok(())
    }
}
