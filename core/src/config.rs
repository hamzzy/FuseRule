use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use crate::rule_graph::chain::TriggerCondition;
use crate::rule_graph::conditional_action::ActionCondition;
use crate::rule_graph::correlation::{CorrelationPattern, TemporalWindow};
use crate::rule_graph::dynamic_predicate::PredicateSource;

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
    /// NEW: Correlation patterns for cross-rule temporal matching
    #[serde(default)]
    pub correlation_patterns: Vec<CorrelationPatternConfig>,
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
    /// NEW: Conditional actions (optional, overrides single action if present)
    #[serde(default)]
    pub actions: Vec<ConditionalActionConfig>,
    /// NEW: Downstream rules for DAG execution
    #[serde(default)]
    pub downstream_rules: Vec<DownstreamRuleConfig>,
    /// NEW: Dynamic predicate configuration (optional)
    #[serde(default)]
    pub dynamic_predicate: Option<DynamicPredicateConfig>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_with_conditional_actions() {
        let yaml = r#"
engine:
  persistence_path: "/tmp/test"
  max_pending_batches: 1000
  agent_concurrency: 10

schema:
  - name: "price"
    data_type: "float64"
  - name: "severity"
    data_type: "utf8"

agents:
  - name: "slack"
    type: "webhook"
    url: "https://slack.com/webhook"
  - name: "pagerduty"
    type: "webhook"
    url: "https://pagerduty.com/webhook"

rules:
  - id: "high_price"
    name: "High Price Alert"
    predicate: "price > 100"
    action: "slack"
    actions:
      - condition:
          type: "FieldEquals"
          field: "severity"
          value: "critical"
        agent: "pagerduty"
        priority: 10
      - condition:
          type: "FieldEquals"
          field: "severity"
          value: "warning"
        agent: "slack"
        priority: 5
"#;

        let config: FuseRuleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.rules.len(), 1);
        assert_eq!(config.rules[0].actions.len(), 2);
        assert_eq!(config.rules[0].actions[0].agent, "pagerduty");
        assert_eq!(config.rules[0].actions[0].priority, 10);

        // Validate should pass
        config.validate().unwrap();
    }

    #[test]
    fn test_config_with_downstream_rules() {
        let yaml = r#"
engine:
  persistence_path: "/tmp/test"

schema:
  - name: "price"
    data_type: "float64"

agents:
  - name: "logger"
    type: "logger"

rules:
  - id: "rule_a"
    name: "Rule A"
    predicate: "price > 100"
    action: "logger"
    downstream_rules:
      - rule_id: "rule_b"
        condition:
          type: "Always"
  - id: "rule_b"
    name: "Rule B"
    predicate: "price > 50"
    action: "logger"
"#;

        let config: FuseRuleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.rules[0].downstream_rules.len(), 1);
        assert_eq!(config.rules[0].downstream_rules[0].rule_id, "rule_b");

        // Validate should pass
        config.validate().unwrap();
    }

    #[test]
    fn test_config_with_correlation_patterns() {
        let yaml = r#"
engine:
  persistence_path: "/tmp/test"

schema:
  - name: "price"
    data_type: "float64"

agents:
  - name: "logger"
    type: "logger"

rules:
  - id: "rule_a"
    name: "Rule A"
    predicate: "price > 100"
    action: "logger"
  - id: "rule_b"
    name: "Rule B"
    predicate: "price > 200"
    action: "logger"
  - id: "meta_rule"
    name: "Meta Rule"
    predicate: "price > 0"
    action: "logger"

correlation_patterns:
  - type: "Sequence"
    rule_ids: ["rule_a", "rule_b"]
    window:
      duration_seconds: 300
      window_type: "Sliding"
    meta_rule_id: "meta_rule"
"#;

        let config: FuseRuleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.correlation_patterns.len(), 1);

        // Validate should pass
        config.validate().unwrap();
    }

    #[test]
    fn test_config_validation_missing_downstream_rule() {
        let yaml = r#"
engine:
  persistence_path: "/tmp/test"

schema:
  - name: "price"
    data_type: "float64"

agents:
  - name: "logger"
    type: "logger"

rules:
  - id: "rule_a"
    name: "Rule A"
    predicate: "price > 100"
    action: "logger"
    downstream_rules:
      - rule_id: "nonexistent"
        condition:
          type: "Always"
"#;

        let config: FuseRuleConfig = serde_yaml::from_str(yaml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonexistent"));
    }

    #[test]
    fn test_config_validation_cycle_detection() {
        let yaml = r#"
engine:
  persistence_path: "/tmp/test"

schema:
  - name: "price"
    data_type: "float64"

agents:
  - name: "logger"
    type: "logger"

rules:
  - id: "rule_a"
    name: "Rule A"
    predicate: "price > 100"
    action: "logger"
    downstream_rules:
      - rule_id: "rule_b"
  - id: "rule_b"
    name: "Rule B"
    predicate: "price > 50"
    action: "logger"
    downstream_rules:
      - rule_id: "rule_a"
"#;

        let config: FuseRuleConfig = serde_yaml::from_str(yaml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("cycle") || error_msg.contains("Cycle"));
    }

    #[test]
    fn test_config_validation_missing_meta_rule() {
        let yaml = r#"
engine:
  persistence_path: "/tmp/test"

schema:
  - name: "price"
    data_type: "float64"

agents:
  - name: "logger"
    type: "logger"

rules:
  - id: "rule_a"
    name: "Rule A"
    predicate: "price > 100"
    action: "logger"

correlation_patterns:
  - type: "All"
    rule_ids: ["rule_a"]
    window:
      duration_seconds: 60
      window_type: "Sliding"
    meta_rule_id: "nonexistent"
"#;

        let config: FuseRuleConfig = serde_yaml::from_str(yaml).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonexistent"));
    }

    #[test]
    fn test_config_with_dynamic_predicate() {
        let yaml = r#"
engine:
  persistence_path: "/tmp/test"

schema:
  - name: "price"
    data_type: "float64"

agents:
  - name: "logger"
    type: "logger"

rules:
  - id: "rule_a"
    name: "Rule A"
    predicate: "price > 100"
    action: "logger"
    dynamic_predicate:
      source:
        type: "Http"
        url: "https://api.example.com/predicates"
      refresh_interval_seconds: 300
      versions:
        v1: "price > 100"
        v2: "price > 200"
      active_version: "v1"
      feature_flags:
        tenant_premium: "v2"
"#;

        let config: FuseRuleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.rules.len(), 1);
        assert!(config.rules[0].dynamic_predicate.is_some());

        let dp = config.rules[0].dynamic_predicate.as_ref().unwrap();
        assert_eq!(dp.versions.len(), 2);
        assert_eq!(dp.active_version, Some("v1".to_string()));
        assert_eq!(dp.feature_flags.get("tenant_premium"), Some(&"v2".to_string()));

        // Validate should pass
        config.validate().unwrap();
    }

    #[test]
    fn test_backward_compatible_config() {
        // Old config without any new features should still work
        let yaml = r#"
engine:
  persistence_path: "/tmp/test"

schema:
  - name: "price"
    data_type: "float64"

agents:
  - name: "logger"
    type: "logger"

rules:
  - id: "simple_rule"
    name: "Simple Rule"
    predicate: "price > 100"
    action: "logger"
"#;

        let config: FuseRuleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.rules.len(), 1);
        assert_eq!(config.rules[0].actions.len(), 0);
        assert_eq!(config.rules[0].downstream_rules.len(), 0);
        assert!(config.rules[0].dynamic_predicate.is_none());
        assert_eq!(config.correlation_patterns.len(), 0);

        // Validate should pass
        config.validate().unwrap();
    }
}

// ============================================================================
// NEW: Advanced Rule Graph Configuration Structures
// ============================================================================

/// Configuration for conditional actions
#[derive(Debug, Deserialize, Clone)]
pub struct ConditionalActionConfig {
    /// Condition that must be true to execute this action
    pub condition: ActionCondition,
    /// Agent to execute if condition is met
    pub agent: String,
    /// Optional parameters to pass to the agent
    #[serde(default)]
    pub parameters: Option<HashMap<String, serde_json::Value>>,
    /// Priority (higher = executed first)
    #[serde(default)]
    pub priority: i32,
}

/// Configuration for downstream rule in DAG
#[derive(Debug, Deserialize, Clone)]
pub struct DownstreamRuleConfig {
    /// ID of the downstream rule to trigger
    pub rule_id: String,
    /// Condition for triggering this downstream rule
    #[serde(default = "default_trigger_condition")]
    pub condition: TriggerCondition,
}

fn default_trigger_condition() -> TriggerCondition {
    TriggerCondition::Always
}

/// Configuration for dynamic predicates
#[derive(Debug, Deserialize, Clone)]
pub struct DynamicPredicateConfig {
    /// Source for loading the predicate
    pub source: PredicateSource,
    /// Refresh interval in seconds
    pub refresh_interval_seconds: Option<u64>,
    /// Predicate versions for A/B testing
    #[serde(default)]
    pub versions: HashMap<String, String>,
    /// Active version ID
    pub active_version: Option<String>,
    /// Feature flags (tenant_id -> version_id)
    #[serde(default)]
    pub feature_flags: HashMap<String, String>,
}

/// Configuration for correlation patterns
#[derive(Debug, Deserialize, Clone)]
pub struct CorrelationPatternConfig {
    /// The correlation pattern
    #[serde(flatten)]
    pub pattern: CorrelationPattern,
    /// Meta-rule to trigger when pattern matches
    pub meta_rule_id: String,
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
        use std::collections::HashSet;

        // Validate agents
        let mut agent_names = HashSet::new();
        for agent in &self.agents {
            if !agent_names.insert(&agent.name) {
                anyhow::bail!("Duplicate agent name: {}", agent.name);
            }
            if agent.r#type == "webhook" && agent.url.is_none() {
                anyhow::bail!("Webhook agent '{}' missing URL", agent.name);
            }
        }

        // Validate rules
        let mut rule_ids = HashSet::new();
        for rule in &self.rules {
            if !rule_ids.insert(&rule.id) {
                anyhow::bail!("Duplicate rule ID: {}", rule.id);
            }

            // Validate single action
            if !agent_names.contains(&rule.action) {
                anyhow::bail!(
                    "Rule '{}' references unknown agent '{}'",
                    rule.id,
                    rule.action
                );
            }

            // NEW: Validate conditional actions
            for cond_action in &rule.actions {
                if !agent_names.contains(&cond_action.agent) {
                    anyhow::bail!(
                        "Rule '{}' conditional action references unknown agent '{}'",
                        rule.id,
                        cond_action.agent
                    );
                }
            }

            if rule.predicate.trim().is_empty() {
                anyhow::bail!("Rule '{}' has an empty predicate", rule.id);
            }
        }

        // NEW: Validate downstream rule references
        for rule in &self.rules {
            for downstream in &rule.downstream_rules {
                if !rule_ids.contains(&downstream.rule_id) {
                    anyhow::bail!(
                        "Rule '{}' has downstream rule '{}' which doesn't exist",
                        rule.id,
                        downstream.rule_id
                    );
                }
            }
        }

        // NEW: Validate no cycles in DAG
        if self.rules.iter().any(|r| !r.downstream_rules.is_empty()) {
            use crate::rule_graph::chain::{ChainedRule, RuleChain};

            let mut chain = RuleChain::new();
            for rule in &self.rules {
                if !rule.downstream_rules.is_empty() {
                    let mut trigger_conditions = HashMap::new();
                    for downstream in &rule.downstream_rules {
                        trigger_conditions.insert(downstream.rule_id.clone(), downstream.condition.clone());
                    }

                    chain.add_chain(ChainedRule {
                        rule_id: rule.id.clone(),
                        downstream_rules: rule.downstream_rules.iter().map(|d| d.rule_id.clone()).collect(),
                        trigger_conditions,
                    })?;
                }
            }

            // This will error if there's a cycle
            chain.validate_dag()?;
        }

        // NEW: Validate correlation patterns
        for pattern_config in &self.correlation_patterns {
            // Verify meta_rule_id exists
            if !rule_ids.contains(&pattern_config.meta_rule_id) {
                anyhow::bail!(
                    "Correlation pattern references unknown meta_rule_id '{}'",
                    pattern_config.meta_rule_id
                );
            }

            // Verify all rule_ids in pattern exist
            let pattern_rule_ids = match &pattern_config.pattern {
                CorrelationPattern::All { rule_ids, .. } => rule_ids,
                CorrelationPattern::Any { rule_ids, .. } => rule_ids,
                CorrelationPattern::Sequence { rule_ids, .. } => rule_ids,
                CorrelationPattern::NotPattern { required_rule, excluded_rule, .. } => {
                    // Check both rules individually
                    if !rule_ids.contains(required_rule) {
                        anyhow::bail!(
                            "Correlation pattern references unknown rule '{}'",
                            required_rule
                        );
                    }
                    if !rule_ids.contains(excluded_rule) {
                        anyhow::bail!(
                            "Correlation pattern references unknown rule '{}'",
                            excluded_rule
                        );
                    }
                    continue;
                }
            };

            for pattern_rule_id in pattern_rule_ids {
                if !rule_ids.contains(pattern_rule_id) {
                    anyhow::bail!(
                        "Correlation pattern references unknown rule '{}'",
                        pattern_rule_id
                    );
                }
            }
        }

        if self.schema.is_empty() {
            anyhow::bail!("Configuration must define a schema");
        }

        Ok(())
    }
}
