//! # FuseRule - High-Performance Rule Engine
//!
//! FuseRule is a high-performance, Arrow-native Complex Event Processing (CEP) engine
//! with SQL-powered rules for real-time data auditing and event processing.
//!
//! ## Quick Start
//!
//! ```no_run
//! use fuse_rule_core::{RuleEngine, config::FuseRuleConfig};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = FuseRuleConfig::from_file("fuse_rule_config.yaml")?;
//! let mut engine = RuleEngine::from_config(config).await?;
//! // Process batches and evaluate rules...
//! # Ok(())
//! # }
//! ```
//!
//! ## Features
//!
//! - **Arrow-Native**: Zero-copy columnar data processing
//! - **SQL-Powered Rules**: Write predicates using standard SQL
//! - **Stateful Transitions**: Track rule activation/deactivation
//! - **Time Windows**: Sliding windows for aggregate functions
//! - **Pluggable Architecture**: Custom state stores, evaluators, and agents


pub mod agent;
pub mod agent_queue;
pub mod config;
pub mod coverage;
pub mod evaluator;
pub mod metrics;
pub mod observability;
pub mod rule;
pub mod rule_graph;
pub mod state;
pub mod udf;
pub mod window;

use crate::agent::{Activation, Agent};
use crate::agent_queue::{AgentQueue, AgentTask, CircuitBreaker};
use crate::config::FuseRuleConfig;
use crate::evaluator::{CompiledRuleEdge, RuleEvaluator};
use crate::rule::Rule;
use crate::state::{PredicateResult, RuleTransition, StateStore};
use crate::window::WindowBuffer;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// A trace of what happened during an evaluation batch.
///
/// This provides observability into rule evaluation, showing which rules
/// were evaluated, their results, and whether actions were fired.
///
/// # Example
///
/// ```no_run
/// # use fuse_rule_core::RuleEngine;
/// # use arrow::array::Float64Array;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use arrow::record_batch::RecordBatch;
/// # use std::sync::Arc;
/// # async fn example(engine: &mut RuleEngine) -> anyhow::Result<()> {
/// // Create a test batch
/// let schema = Schema::new(vec![Field::new("price", DataType::Float64, true)]);
/// let batch = RecordBatch::try_new(
///     Arc::new(schema),
///     vec![Arc::new(Float64Array::from(vec![150.0, 50.0]))],
/// )?;
/// let traces = engine.process_batch(&batch).await?;
/// for trace in traces {
///     if trace.action_fired {
///         println!("Rule '{}' activated!", trace.rule_name);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, serde::Serialize)]
pub struct EvaluationTrace {
    /// Unique identifier for the rule
    pub rule_id: String,
    /// Human-readable rule name
    pub rule_name: String,
    /// Rule version number
    pub rule_version: u32,
    /// Result of predicate evaluation
    pub result: PredicateResult,
    /// State transition: "None", "Activated", or "Deactivated"
    pub transition: String,
    /// Whether the action/agent was fired
    pub action_fired: bool,
    /// Optional status message from the agent
    pub agent_status: Option<String>,
}

/// The core rule engine that evaluates rules against data batches.
///
/// `RuleEngine` is the main entry point for using FuseRule. It manages
/// rules, state, agents, and provides methods to process data batches.
///
/// # Example
///
/// ```no_run
/// use fuse_rule_core::{RuleEngine, config::FuseRuleConfig};
/// use arrow::array::Float64Array;
/// use arrow::datatypes::{DataType, Field, Schema};
/// use arrow::record_batch::RecordBatch;
/// use std::sync::Arc;
///
/// # async fn example() -> anyhow::Result<()> {
/// // Create engine from configuration
/// let config = FuseRuleConfig::from_file("config.yaml")?;
/// let mut engine = RuleEngine::from_config(config).await?;
///
/// // Create a test batch
/// let schema = Schema::new(vec![Field::new("price", DataType::Float64, true)]);
/// let batch = RecordBatch::try_new(
///     Arc::new(schema),
///     vec![Arc::new(Float64Array::from(vec![150.0, 50.0]))],
/// )?;
///
/// // Process a batch of data
/// let traces = engine.process_batch(&batch).await?;
/// # Ok(())
/// # }
/// ```
pub struct RuleEngine {
    pub evaluator: Box<dyn RuleEvaluator>,
    pub state: Box<dyn StateStore>,
    pub rules: Vec<CompiledRuleEdge>,
    pub window_buffers: HashMap<String, WindowBuffer>,
    pub agents: HashMap<String, Arc<dyn Agent>>,
    pub schema: Arc<arrow::datatypes::Schema>,
    pub agent_queue: Option<AgentQueue>,
    pub circuit_breakers: HashMap<String, Arc<CircuitBreaker>>,
    pub audit_log: Option<Arc<observability::AuditLog>>,
    pub trace_store: Option<Arc<observability::TraceStore>>,
    /// NEW: DAG executor for rule chaining and cascading
    pub dag_executor: Option<rule_graph::dag::DAGExecutor>,
    /// NEW: Correlation engine for temporal pattern matching
    pub correlation_engine: Option<Arc<tokio::sync::RwLock<rule_graph::correlation::CorrelationEngine>>>,
    /// NEW: Predicate loader for dynamic predicates
    pub predicate_loader: Option<rule_graph::dynamic_predicate::PredicateLoader>,
    /// NEW: Conditional action sets per rule
    pub conditional_actions: HashMap<String, rule_graph::conditional_action::ConditionalActionSet>,
    /// NEW: Mapping from correlation pattern index to meta_rule_id
    pub correlation_pattern_meta_rules: Vec<String>,
}

impl RuleEngine {
    pub fn new(
        evaluator: Box<dyn RuleEvaluator>,
        state: Box<dyn StateStore>,
        schema: Arc<arrow::datatypes::Schema>,
        max_pending_batches: usize,
        agent_concurrency: usize,
    ) -> Self {
        // Create agent queue with bounded channel for backpressure
        let (agent_queue, worker) = AgentQueue::new(Some(max_pending_batches));
        let worker = worker.with_concurrency(agent_concurrency);
        // Spawn worker in background
        let _worker_handle = tokio::spawn(async move {
            worker.run().await;
        });
        // Note: worker_handle is dropped but task continues running

        Self {
            evaluator,
            state,
            rules: Vec::new(),
            window_buffers: HashMap::new(),
            agents: HashMap::new(),
            schema,
            agent_queue: Some(agent_queue),
            circuit_breakers: HashMap::new(),
            audit_log: None,
            trace_store: None,
            // NEW: Initialize advanced features as None/empty
            dag_executor: None,
            correlation_engine: None,
            predicate_loader: None,
            conditional_actions: HashMap::new(),
            correlation_pattern_meta_rules: Vec::new(),
        }
    }

    pub fn get_or_create_circuit_breaker(&mut self, agent_name: &str) -> Arc<CircuitBreaker> {
        self.circuit_breakers
            .entry(agent_name.to_string())
            .or_insert_with(|| {
                Arc::new(CircuitBreaker::new(
                    5, // 5 failures before opening
                    std::time::Duration::from_secs(30),
                ))
            })
            .clone()
    }

    pub async fn from_config(config: FuseRuleConfig) -> Result<Self> {
        // 1. Build Schema (with evolution support - fields can be added/removed)
        let mut fields = Vec::new();
        for f in config.schema {
            let dt = match f.data_type.as_str() {
                "int32" => arrow::datatypes::DataType::Int32,
                "int64" => arrow::datatypes::DataType::Int64,
                "float32" => arrow::datatypes::DataType::Float32,
                "float64" => arrow::datatypes::DataType::Float64,
                "bool" => arrow::datatypes::DataType::Boolean,
                "utf8" | "string" => arrow::datatypes::DataType::Utf8,
                _ => arrow::datatypes::DataType::Utf8,
            };
            fields.push(arrow::datatypes::Field::new(f.name, dt, true));
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        // 2. Build Components (Edges)
        let evaluator = Box::new(crate::evaluator::DataFusionEvaluator::new());
        let state_store = crate::state::SledStateStore::new(&config.engine.persistence_path)?;

        // Start state cleanup task for rules with TTL
        let mut rules_ttl = std::collections::HashMap::new();
        for rule in &config.rules {
            if let Some(ttl) = rule.state_ttl_seconds {
                rules_ttl.insert(rule.id.clone(), ttl);
            }
        }
        if !rules_ttl.is_empty() {
            state_store.start_cleanup_task(rules_ttl);
        }

        let state = Box::new(state_store);

        let max_pending = config.engine.max_pending_batches;
        let agent_concurrency = config.engine.agent_concurrency;
        let mut engine = Self::new(
            evaluator,
            state,
            Arc::clone(&schema),
            max_pending,
            agent_concurrency,
        );

        // 3. Initialize Observability Components
        if config.observability.audit_log_enabled {
            let db = sled::open(&config.engine.persistence_path)?;
            let audit_log = Arc::new(observability::AuditLog::new(Arc::new(db))?);
            engine.audit_log = Some(audit_log.clone());

            // Log engine start event
            let event = observability::AuditEvent::new(
                observability::AuditEventType::EngineStarted,
                "FuseRule engine started",
            );
            let _ = audit_log.log(event).await;
        }

        if config.observability.tracing.enabled {
            // Initialize OpenTelemetry tracing
            let tracing_config = observability::tracing::TracingConfig {
                enabled: config.observability.tracing.enabled,
                otlp_endpoint: config.observability.tracing.otlp_endpoint,
                service_name: config.observability.tracing.service_name,
                sampling_ratio: config.observability.tracing.sampling_ratio,
                resource_attributes: None,
            };

            if let Ok(Some(_provider)) = observability::init_tracing(tracing_config) {
                info!("Distributed tracing initialized");
            }
        }

        if config.observability.trace_store.enabled {
            let trace_store_config = observability::trace_store::TraceStoreConfig {
                enabled: config.observability.trace_store.enabled,
                clickhouse_url: config.observability.trace_store.clickhouse_url,
                database: config.observability.trace_store.database,
                table: config.observability.trace_store.table,
                batch_size: 1000,
                flush_interval_seconds: 10,
            };

            match observability::TraceStore::new(trace_store_config).await {
                Ok(trace_store) => {
                    engine.trace_store = Some(Arc::new(trace_store));
                    info!("Trace store initialized");
                }
                Err(e) => {
                    warn!("Failed to initialize trace store: {}", e);
                }
            }
        }

        // 4. Add Agents - DECOUPLED
        // Agents must be added by the caller (CLI) as Core does not know about specific agents.

        // 4.5. NEW: Initialize Advanced Rule Graph Components

        // Build PredicateLoader if any rule has dynamic predicates
        if config.rules.iter().any(|r| r.dynamic_predicate.is_some()) {
            use rule_graph::dynamic_predicate::{DynamicPredicate, PredicateLoader};

            let loader = PredicateLoader::new();
            for r_cfg in &config.rules {
                if let Some(dp_config) = &r_cfg.dynamic_predicate {
                    let dynamic_pred = DynamicPredicate {
                        rule_id: r_cfg.id.clone(),
                        source: dp_config.source.clone(),
                        versions: dp_config.versions.clone(),
                        active_version: dp_config.active_version.clone(),
                        feature_flags: dp_config.feature_flags.clone(),
                        refresh_interval_seconds: dp_config.refresh_interval_seconds,
                    };
                    loader.register(dynamic_pred).await;
                }
            }

            // Start refresh background task if configured
            // Use first rule's refresh interval as default
            if let Some(interval) = config.rules.iter()
                .filter_map(|r| r.dynamic_predicate.as_ref())
                .filter_map(|dp| dp.refresh_interval_seconds)
                .next()
            {
                loader.start_refresh_task(interval);
            }

            engine.predicate_loader = Some(loader);
            info!("Dynamic predicate loader initialized");
        }

        // Build DAGExecutor if any rule has downstream dependencies
        if config.rules.iter().any(|r| !r.downstream_rules.is_empty()) {
            use rule_graph::chain::{ChainedRule, RuleChain};
            use rule_graph::dag::{DAGExecutor, RuleDAG};

            let mut chain = RuleChain::new();
            for r_cfg in &config.rules {
                if !r_cfg.downstream_rules.is_empty() {
                    let mut trigger_conditions = HashMap::new();
                    for downstream in &r_cfg.downstream_rules {
                        trigger_conditions.insert(downstream.rule_id.clone(), downstream.condition.clone());
                    }

                    chain.add_chain(ChainedRule {
                        rule_id: r_cfg.id.clone(),
                        downstream_rules: r_cfg.downstream_rules.iter().map(|d| d.rule_id.clone()).collect(),
                        trigger_conditions,
                    })?;
                }
            }

            let dag = RuleDAG::new(chain)?;
            engine.dag_executor = Some(DAGExecutor::new(dag));
            info!("DAG executor initialized");
        }

        // Build CorrelationEngine if patterns defined
        if !config.correlation_patterns.is_empty() {
            use rule_graph::correlation::CorrelationEngine;

            let mut corr_engine = CorrelationEngine::new(1000); // max 1000 events per rule
            for pattern_config in &config.correlation_patterns {
                corr_engine.add_pattern(pattern_config.pattern.clone());
                engine.correlation_pattern_meta_rules.push(pattern_config.meta_rule_id.clone());
            }

            engine.correlation_engine = Some(Arc::new(tokio::sync::RwLock::new(corr_engine)));
            info!("Correlation engine initialized with {} patterns", config.correlation_patterns.len());
        }

        // Build ConditionalActionSets per rule
        for r_cfg in &config.rules {
            if !r_cfg.actions.is_empty() {
                use rule_graph::conditional_action::{ConditionalAction, ConditionalActionSet};

                let mut action_set = ConditionalActionSet::new();
                for action_config in &r_cfg.actions {
                    action_set.add_action(ConditionalAction {
                        condition: action_config.condition.clone(),
                        agent: action_config.agent.clone(),
                        parameters: action_config.parameters.clone(),
                        priority: action_config.priority,
                    });
                }
                engine.conditional_actions.insert(r_cfg.id.clone(), action_set);
            }
        }

        // 5. Add Rules
        for r_cfg in config.rules {
            engine
                .add_rule(Rule {
                    id: r_cfg.id.clone(),
                    name: r_cfg.name,
                    predicate: r_cfg.predicate,
                    action: r_cfg.action,
                    window_seconds: r_cfg.window_seconds,
                    version: r_cfg.version,
                    enabled: r_cfg.enabled,
                    description: r_cfg.description,
                    tags: r_cfg.tags,
                    // NEW: Add new fields
                    conditional_actions: r_cfg.actions.iter().map(|a| {
                        use rule_graph::conditional_action::ConditionalAction;
                        ConditionalAction {
                            condition: a.condition.clone(),
                            agent: a.agent.clone(),
                            parameters: a.parameters.clone(),
                            priority: a.priority,
                        }
                    }).collect(),
                    downstream_rules: r_cfg.downstream_rules.iter().map(|d| d.rule_id.clone()).collect(),
                    has_dynamic_predicate: r_cfg.dynamic_predicate.is_some(),
                })
                .await?;
        }

        Ok(engine)
    }

    pub async fn reload_from_config(&mut self, config: FuseRuleConfig) -> Result<()> {
        info!("🔄 Reloading engine configuration...");

        let mut new_rules = Vec::new();
        let mut new_window_buffers = HashMap::new();

        for r_cfg in config.rules {
            let rule = Rule {
                id: r_cfg.id.clone(),
                name: r_cfg.name,
                predicate: r_cfg.predicate,
                action: r_cfg.action,
                window_seconds: r_cfg.window_seconds,
                version: r_cfg.version,
                enabled: r_cfg.enabled,
                description: r_cfg.description,
                tags: r_cfg.tags,
                // NEW: Add new fields
                conditional_actions: r_cfg.actions.iter().map(|a| {
                    use rule_graph::conditional_action::ConditionalAction;
                    ConditionalAction {
                        condition: a.condition.clone(),
                        agent: a.agent.clone(),
                        parameters: a.parameters.clone(),
                        priority: a.priority,
                    }
                }).collect(),
                downstream_rules: r_cfg.downstream_rules.iter().map(|d| d.rule_id.clone()).collect(),
                has_dynamic_predicate: r_cfg.dynamic_predicate.is_some(),
            };

            // Preserve existing window buffers if possible, otherwise create new
            if let Some(secs) = rule.window_seconds {
                if let Some(existing_buffer) = self.window_buffers.remove(&rule.id) {
                    new_window_buffers.insert(rule.id.clone(), existing_buffer);
                } else {
                    new_window_buffers.insert(rule.id.clone(), WindowBuffer::new(secs));
                }
            }

            let compiled = self.evaluator.compile(rule, &self.schema)?;
            new_rules.push(compiled);
        }

        self.rules = new_rules;
        self.window_buffers = new_window_buffers;

        info!(
            "✅ Engine reloaded: {} rules, {} agents",
            self.rules.len(),
            self.agents.len()
        );
        Ok(())
    }

    pub fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        Arc::clone(&self.schema)
    }

    pub fn add_agent(&mut self, name: String, agent: Arc<dyn Agent>) {
        self.agents.insert(name, agent);
    }

    /// Set audit log for tracking rule lifecycle events
    pub fn with_audit_log(mut self, audit_log: Arc<observability::AuditLog>) -> Self {
        self.audit_log = Some(audit_log);
        self
    }

    /// Set trace store for persisting evaluation traces
    pub fn with_trace_store(mut self, trace_store: Arc<observability::TraceStore>) -> Self {
        self.trace_store = Some(trace_store);
        self
    }

    pub async fn add_rule(&mut self, rule: Rule) -> Result<()> {
        if let Some(secs) = rule.window_seconds {
            self.window_buffers
                .insert(rule.id.clone(), WindowBuffer::new(secs));
        }
        let compiled = self.evaluator.compile(rule.clone(), &self.schema)?;
        self.rules.push(compiled);

        // Emit audit event
        if let Some(audit_log) = &self.audit_log {
            let event = observability::AuditEvent::new(
                observability::AuditEventType::RuleCreated,
                format!("Rule '{}' created", rule.name),
            )
            .with_rule(&rule.id, &rule.name)
            .with_metadata(serde_json::json!({
                "predicate": rule.predicate,
                "action": rule.action,
                "version": rule.version,
                "enabled": rule.enabled,
            }));

            let _ = audit_log.log(event).await;
        }

        Ok(())
    }

    pub async fn update_rule(&mut self, rule_id: &str, new_rule: Rule) -> Result<()> {
        // Find existing rule
        let rule_idx = self.rules.iter().position(|r| r.rule.id == rule_id);
        if rule_idx.is_none() {
            anyhow::bail!("Rule not found: {}", rule_id);
        }

        let rule_idx = rule_idx.unwrap();

        // Get old rule info before modifying
        let old_version = self.rules[rule_idx].rule.version;
        let old_window_seconds = self.rules[rule_idx].rule.window_seconds;

        // Preserve window buffer if window_seconds unchanged
        let preserve_buffer = old_window_seconds == new_rule.window_seconds;
        let existing_buffer = if preserve_buffer {
            self.window_buffers.remove(rule_id)
        } else {
            None
        };

        // Compile new rule
        let compiled = self.evaluator.compile(new_rule.clone(), &self.schema)?;

        // Replace rule
        self.rules[rule_idx] = compiled;

        // Restore or create window buffer
        if let Some(buffer) = existing_buffer {
            self.window_buffers.insert(rule_id.to_string(), buffer);
        } else if let Some(secs) = self.rules[rule_idx].rule.window_seconds {
            self.window_buffers
                .insert(rule_id.to_string(), WindowBuffer::new(secs));
        }

        // Emit audit event
        if let Some(audit_log) = &self.audit_log {
            let event = observability::AuditEvent::new(
                observability::AuditEventType::RuleUpdated,
                format!("Rule '{}' updated", new_rule.name),
            )
            .with_rule(&new_rule.id, &new_rule.name)
            .with_metadata(serde_json::json!({
                "old_version": old_version,
                "new_version": new_rule.version,
                "predicate": new_rule.predicate,
            }));

            let _ = audit_log.log(event).await;
        }

        Ok(())
    }

    pub async fn toggle_rule(&mut self, rule_id: &str, enabled: bool) -> Result<()> {
        let rule_idx = self.rules.iter().position(|r| r.rule.id == rule_id);
        if let Some(idx) = rule_idx {
            // Get rule info before modifying
            let rule_id_str = self.rules[idx].rule.id.clone();
            let rule_name_str = self.rules[idx].rule.name.clone();

            // Update enabled state
            self.rules[idx].rule.enabled = enabled;

            // Emit audit event
            if let Some(audit_log) = &self.audit_log {
                let event_type = if enabled {
                    observability::AuditEventType::RuleEnabled
                } else {
                    observability::AuditEventType::RuleDisabled
                };

                let event = observability::AuditEvent::new(
                    event_type,
                    format!("Rule '{}' {}", rule_name_str, if enabled { "enabled" } else { "disabled" }),
                )
                .with_rule(&rule_id_str, &rule_name_str);

                let _ = audit_log.log(event).await;
            }

            Ok(())
        } else {
            anyhow::bail!("Rule not found: {}", rule_id)
        }
    }

    /// Extract runtime context from RecordBatch for condition evaluation
    ///
    /// Builds a HashMap containing:
    /// - Field values from the first matched row
    /// - Metadata (rule_id, rule_name, tenant_id if provided)
    fn build_runtime_context(
        batch: &RecordBatch,
        rule: &Rule,
        tenant_id: Option<&str>,
    ) -> Result<HashMap<String, serde_json::Value>> {
        use arrow::array::*;

        let mut context = HashMap::new();

        // Extract values from first row (if batch has rows)
        if batch.num_rows() > 0 {
            for (i, field) in batch.schema().fields().iter().enumerate() {
                let column = batch.column(i);
                let field_name = field.name();

                // Extract value based on data type
                let value = match field.data_type() {
                    arrow::datatypes::DataType::Float64 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
                            if !arr.is_null(0) {
                                Some(serde_json::json!(arr.value(0)))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    arrow::datatypes::DataType::Float32 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Float32Array>() {
                            if !arr.is_null(0) {
                                Some(serde_json::json!(arr.value(0) as f64))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    arrow::datatypes::DataType::Int64 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                            if !arr.is_null(0) {
                                Some(serde_json::json!(arr.value(0)))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    arrow::datatypes::DataType::Int32 => {
                        if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
                            if !arr.is_null(0) {
                                Some(serde_json::json!(arr.value(0) as i64))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    arrow::datatypes::DataType::Utf8 => {
                        if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
                            if !arr.is_null(0) {
                                Some(serde_json::json!(arr.value(0)))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    arrow::datatypes::DataType::Boolean => {
                        if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
                            if !arr.is_null(0) {
                                Some(serde_json::json!(arr.value(0)))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None, // Unsupported type
                };

                if let Some(v) = value {
                    context.insert(field_name.clone(), v);
                }
            }
        }

        // Add metadata
        context.insert("rule_id".to_string(), serde_json::json!(rule.id));
        context.insert("rule_name".to_string(), serde_json::json!(rule.name));

        if let Some(tid) = tenant_id {
            context.insert("tenant_id".to_string(), serde_json::json!(tid));
        }

        Ok(context)
    }

    /// Evaluate a single rule (used for DAG cascading)
    ///
    /// This is called when a downstream rule is triggered by DAG execution.
    /// It evaluates the rule, updates state, and may trigger further cascades.
    fn evaluate_single_rule<'a>(
        &'a mut self,
        rule_id: &'a str,
        batch: &'a RecordBatch,
        tenant_id: Option<&'a str>,
        triggered_rules: &'a mut std::collections::HashSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<EvaluationTrace>>> + 'a>> {
        Box::pin(async move {
        // Prevent infinite loops
        if triggered_rules.contains(rule_id) {
            warn!("Detected potential cycle in DAG - rule '{}' already triggered in this cascade", rule_id);
            return Ok(None);
        }
        triggered_rules.insert(rule_id.to_string());

        // Find the rule
        let rule_compiled = self.rules.iter().find(|r| r.rule.id == rule_id);
        if rule_compiled.is_none() {
            warn!("Downstream rule '{}' not found", rule_id);
            return Ok(None);
        }

        let rule_compiled = rule_compiled.unwrap().clone();
        let rule = &rule_compiled.rule;

        if !rule.enabled {
            debug!("Downstream rule '{}' is disabled, skipping", rule_id);
            return Ok(None);
        }

        // Get windowed data if applicable
        let windowed_data = if let Some(buffer) = self.window_buffers.get(rule_id) {
            buffer.get_batches()
        } else {
            vec![]
        };

        // Evaluate the rule
        let results = self.evaluator
            .evaluate_batch(batch, &[rule_compiled.clone()], &[windowed_data.clone()])
            .await?;

        if results.is_empty() {
            return Ok(None);
        }

        let (result, context) = &results[0];

        // Update state
        let transition = self.state.update_result(rule_id, *result).await?;

        let mut trace = EvaluationTrace {
            rule_id: rule.id.clone(),
            rule_name: rule.name.clone(),
            rule_version: rule.version,
            result: *result,
            transition: match transition {
                RuleTransition::None => "None".to_string(),
                RuleTransition::Activated => "Activated".to_string(),
                RuleTransition::Deactivated => "Deactivated".to_string(),
            },
            action_fired: false,
            agent_status: None,
        };

        // If activated, execute actions and cascade
        if let RuleTransition::Activated = transition {
            trace.action_fired = true;

            // Build runtime context
            let runtime_context = Self::build_runtime_context(
                context.as_ref().unwrap_or(batch),
                rule,
                tenant_id,
            )?;

            // Execute conditional actions or single action
            self.execute_actions_for_rule(rule, context.clone(), &runtime_context).await?;

            // DAG Cascading (recursive)
            self.cascade_downstream_rules(rule_id, batch, tenant_id, &runtime_context, triggered_rules).await?;

            // Correlation tracking
            self.record_correlation_event(rule_id, &runtime_context).await?;
        }

        // Update window buffer
        if let Some(buffer) = self.window_buffers.get_mut(rule_id) {
            buffer.add_batch(batch.clone());
        }

        Ok(Some(trace))
        })
    }

    /// Trigger meta-rule when correlation pattern matches
    async fn trigger_meta_rule(&mut self, meta_rule_id: &str) -> Result<()> {
        info!("Correlation pattern matched, triggering meta-rule: {}", meta_rule_id);

        // Find the meta-rule
        let meta_rule = self.rules.iter().find(|r| r.rule.id == meta_rule_id);
        if meta_rule.is_none() {
            warn!("Meta-rule '{}' not found", meta_rule_id);
            return Ok(());
        }

        let meta_rule = meta_rule.unwrap().rule.clone();

        // Create a synthetic activation for the meta-rule
        let activation = Activation {
            rule_id: meta_rule.id.clone(),
            rule_name: meta_rule.name.clone(),
            action: meta_rule.action.clone(),
            context: None, // No specific context for meta-rules
        };

        // Execute the meta-rule's action
        if let Some(agent) = self.agents.get(&meta_rule.action) {
            if let Err(e) = agent.execute(&activation).await {
                error!("Failed to execute meta-rule '{}': {}", meta_rule_id, e);
            } else {
                info!("Meta-rule '{}' executed successfully", meta_rule_id);
            }
        }

        Ok(())
    }

    /// Execute actions for a rule (conditional or single)
    async fn execute_actions_for_rule(
        &mut self,
        rule: &Rule,
        context: Option<RecordBatch>,
        runtime_context: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        use rule_graph::conditional_action::ConditionalAction;

        // Determine which actions to execute
        let actions_to_execute: Vec<ConditionalAction> = if let Some(action_set) = self.conditional_actions.get(&rule.id) {
            // Use conditional actions
            action_set.get_matching_actions(runtime_context)
                .into_iter()
                .map(|a| a.clone())
                .collect()
        } else {
            // Backward compatible: single action
            vec![ConditionalAction {
                condition: rule_graph::conditional_action::ActionCondition::Always,
                agent: rule.action.clone(),
                parameters: None,
                priority: 0,
            }]
        };

        // Execute each matching action
        for action in actions_to_execute {
            let activation = Activation {
                rule_id: rule.id.clone(),
                rule_name: rule.name.clone(),
                action: action.agent.clone(),
                context: context.clone(),
            };

            if let Some(agent_queue) = &self.agent_queue {
                if let Some(agent) = self.agents.get(&action.agent) {
                    let circuit_breaker = self.circuit_breakers.get(&action.agent).cloned();
                    let dlq_sender = Some(agent_queue.dlq_sender.clone());

                    let task = AgentTask::new(
                        activation,
                        agent.clone(),
                        3, // max_retries
                        circuit_breaker,
                        dlq_sender,
                    );

                    if let Err(e) = agent_queue.enqueue(task).await {
                        error!("Failed to enqueue agent task for '{}': {}", action.agent, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Cascade to downstream rules in DAG
    async fn cascade_downstream_rules(
        &mut self,
        rule_id: &str,
        batch: &RecordBatch,
        tenant_id: Option<&str>,
        runtime_context: &HashMap<String, serde_json::Value>,
        triggered_rules: &mut std::collections::HashSet<String>,
    ) -> Result<()> {
        if let Some(dag_executor) = &self.dag_executor {
            let triggered_downstream = dag_executor.get_triggered_rules(rule_id, runtime_context);

            for downstream_id in triggered_downstream {
                debug!("Cascading from '{}' to downstream '{}'", rule_id, downstream_id);

                // Recursively evaluate downstream rule
                if let Err(e) = self.evaluate_single_rule(&downstream_id, batch, tenant_id, triggered_rules).await {
                    error!("Failed to cascade to downstream rule '{}': {}", downstream_id, e);
                }
            }
        }

        Ok(())
    }

    /// Record activation event for correlation tracking
    async fn record_correlation_event(
        &mut self,
        rule_id: &str,
        runtime_context: &HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        if let Some(correlation_engine) = &self.correlation_engine {
            let mut engine = correlation_engine.write().await;

            let event = rule_graph::correlation::RuleActivationEvent {
                rule_id: rule_id.to_string(),
                timestamp: chrono::Utc::now(),
                context: runtime_context.clone(),
            };

            engine.record_activation(event);

            // Check for pattern matches
            let matched_patterns = engine.check_patterns();

            // Trigger meta-rules for matched patterns
            // We need to collect meta_rule_ids first to avoid borrow issues
            let meta_rule_ids: Vec<String> = self.config_correlation_patterns_meta_rules(&matched_patterns);

            drop(engine); // Release lock

            for meta_rule_id in meta_rule_ids {
                if let Err(e) = self.trigger_meta_rule(&meta_rule_id).await {
                    error!("Failed to trigger meta-rule '{}': {}", meta_rule_id, e);
                }
            }
        }

        Ok(())
    }

    /// Helper to get meta_rule_ids from matched patterns
    fn config_correlation_patterns_meta_rules(
        &self,
        matched_patterns: &[&rule_graph::correlation::CorrelationPattern],
    ) -> Vec<String> {
        // Return the meta_rule_ids for the matched patterns
        // The patterns are returned in the same order they were added
        matched_patterns.iter()
            .enumerate()
            .filter_map(|(idx, _)| self.correlation_pattern_meta_rules.get(idx).cloned())
            .collect()
    }

    /// Process a batch of data through the rule engine with advanced features
    ///
    /// # 5-Phase Execution Flow:
    /// 1. **Predicate Resolution**: Load tenant-specific dynamic predicates
    /// 2. **Rule Evaluation**: Evaluate all enabled rules (parallel)
    /// 3. **State Management**: Update state and determine transitions
    /// 4. **Activation Processing**:
    ///    - 4A: Execute conditional actions based on runtime context
    ///    - 4B: Trigger downstream rules (DAG cascading)
    ///    - 4C: Record activation for correlation pattern matching
    /// 5. **Tracing & Observability**: Create traces, persist, emit audit events
    ///
    /// # Parameters
    /// - `batch`: RecordBatch containing data to evaluate
    /// - `tenant_id`: Optional tenant identifier for multi-tenancy (None for single-tenant)
    pub async fn process_batch_with_tenant(
        &mut self,
        batch: &RecordBatch,
        tenant_id: Option<&str>,
    ) -> Result<Vec<EvaluationTrace>> {
        use opentelemetry::trace::{Span, SpanKind, Tracer};

        let _start = Instant::now();

        // Create tracing span for batch ingestion
        let tracer = opentelemetry::global::tracer("fuse-rule-engine");
        let mut batch_span = tracer
            .span_builder("process_batch")
            .with_kind(SpanKind::Server)
            .start(&tracer);

        batch_span.set_attribute(opentelemetry::KeyValue::new(
            "batch.size",
            batch.num_rows() as i64,
        ));

        // ====================================================================
        // PHASE 1: Dynamic Predicate Resolution
        // ====================================================================
        // Load tenant-specific predicates if configured
        let mut resolved_predicates: HashMap<String, String> = HashMap::new();
        if let Some(loader) = &self.predicate_loader {
            for rule in &self.rules {
                if rule.rule.has_dynamic_predicate {
                    if let Some(predicate) = loader.get_predicate(&rule.rule.id, tenant_id).await {
                        debug!(
                            "Resolved dynamic predicate for rule '{}' (tenant: {:?}): {}",
                            rule.rule.id, tenant_id, predicate
                        );
                        resolved_predicates.insert(rule.rule.id.clone(), predicate);
                    }
                }
            }
        }

        // ====================================================================
        // PHASE 2: Rule Evaluation
        // ====================================================================
        let mut windowed_data = Vec::with_capacity(self.rules.len());
        for rule in &self.rules {
            if let Some(buffer) = self.window_buffers.get(&rule.rule.id) {
                windowed_data.push(buffer.get_batches());
            } else {
                windowed_data.push(vec![]);
            }
        }

        // Filter to only enabled rules for evaluation, tracking original indices
        let mut enabled_indices = Vec::new();
        let mut enabled_compiled_rules = Vec::new();
        let mut enabled_window_data = Vec::new();

        for (i, rule) in self.rules.iter().enumerate() {
            if rule.rule.enabled {
                enabled_indices.push(i);
                enabled_compiled_rules.push(rule.clone());
                enabled_window_data.push(windowed_data.get(i).cloned().unwrap_or_default());
            }
        }

        // Parallel rule evaluation
        let evaluation_start = Instant::now();
        let results_with_context = match self
            .evaluator
            .evaluate_batch(batch, &enabled_compiled_rules, &enabled_window_data)
            .await
        {
            Ok(results) => {
                let eval_duration = evaluation_start.elapsed();
                crate::metrics::METRICS.record_evaluation_duration(eval_duration.as_secs_f64());
                results
            }
            Err(e) => {
                error!("Rule evaluation error: {}", e);
                crate::metrics::METRICS.record_evaluation_error();
                return Err(e);
            }
        };
        crate::metrics::METRICS
            .batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // ====================================================================
        // PHASE 3: State Management & Phase 4: Activation Processing
        // ====================================================================
        let mut traces = Vec::new();
        let mut triggered_rules_set = std::collections::HashSet::new();

        // Process enabled rules and create traces for all rules (disabled ones get None transition)
        let enabled_count = enabled_indices.len();
        let mut enabled_results_iter = results_with_context.into_iter();
        let mut enabled_idx_iter = enabled_indices.into_iter();

        // Record per-rule evaluation duration (approximate by dividing total time)
        let eval_duration = evaluation_start.elapsed();
        let per_rule_duration = if enabled_count > 0 {
            eval_duration.as_secs_f64() / enabled_count as f64
        } else {
            0.0
        };

        // Clone rules to avoid borrow issues
        let all_rules: Vec<Rule> = self.rules.iter().map(|r| r.rule.clone()).collect();

        for rule in all_rules.iter() {
            if rule.enabled {
                let _original_idx = enabled_idx_iter.next().unwrap();
                let (result, context) = enabled_results_iter.next().unwrap();

                // Record rule evaluation
                crate::metrics::METRICS.record_rule_evaluation(&rule.id);
                // Record per-rule evaluation duration histogram
                crate::metrics::METRICS
                    .record_rule_evaluation_duration(&rule.id, per_rule_duration);

                let transition = self.state.update_result(&rule.id, result).await?;

                if transition != RuleTransition::None {
                    info!(
                        "Rule '{}' ({} v{}): {:?} -> {:?}",
                        rule.name, rule.id, rule.version, result, transition
                    );
                }

                let mut agent_status = None;
                let mut action_fired = false;

                // PHASE 4: Activation Processing
                if let RuleTransition::Activated = transition {
                    action_fired = true;
                    crate::metrics::METRICS
                        .activations_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::metrics::METRICS.record_rule_activation(&rule.id);

                    // Build runtime context for conditions and cascading
                    let runtime_context = Self::build_runtime_context(
                        context.as_ref().unwrap_or(batch),
                        rule,
                        tenant_id,
                    )?;

                    // 4A: Execute conditional actions (or single action for backward compat)
                    if let Err(e) = self.execute_actions_for_rule(rule, context.clone(), &runtime_context).await {
                        error!("Failed to execute actions for rule '{}': {}", rule.id, e);
                        agent_status = Some(format!("action_failed: {}", e));
                    } else {
                        agent_status = Some("queued".to_string());
                    }

                    // 4B: DAG Cascading - trigger downstream rules
                    if let Err(e) = self.cascade_downstream_rules(
                        &rule.id,
                        batch,
                        tenant_id,
                        &runtime_context,
                        &mut triggered_rules_set,
                    ).await {
                        error!("Failed to cascade from rule '{}': {}", rule.id, e);
                    }

                    // 4C: Correlation Tracking - record activation for pattern matching
                    if let Err(e) = self.record_correlation_event(&rule.id, &runtime_context).await {
                        error!("Failed to record correlation for rule '{}': {}", rule.id, e);
                    }
                }

                traces.push(EvaluationTrace {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    rule_version: rule.version,
                    result,
                    transition: match transition {
                        RuleTransition::None => "None".to_string(),
                        RuleTransition::Activated => "Activated".to_string(),
                        RuleTransition::Deactivated => {
                            crate::metrics::METRICS.record_deactivation();
                            "Deactivated".to_string()
                        }
                    },
                    action_fired,
                    agent_status,
                });

                if let Some(buffer) = self.window_buffers.get_mut(&rule.id) {
                    buffer.add_batch(batch.clone());
                }
            } else {
                // Disabled rule - create trace with None transition
                let last_result = self
                    .state
                    .get_last_result(&rule.id)
                    .await
                    .unwrap_or(PredicateResult::False);
                traces.push(EvaluationTrace {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    rule_version: rule.version,
                    result: last_result,
                    transition: "None".to_string(),
                    action_fired: false,
                    agent_status: Some("rule_disabled".to_string()),
                });
            }
        }

        // Persist traces to trace store if enabled
        if let Some(trace_store) = &self.trace_store {
            if let Err(e) = trace_store.store_traces(&traces).await {
                warn!("Failed to persist traces to store: {}", e);
            }
        }

        // Emit audit events for activations
        if let Some(audit_log) = &self.audit_log {
            for trace in &traces {
                if trace.transition == "Activated" {
                    let event = observability::AuditEvent::new(
                        observability::AuditEventType::RuleActivated,
                        format!("Rule '{}' activated", trace.rule_name),
                    )
                    .with_rule(&trace.rule_id, &trace.rule_name);

                    let _ = audit_log.log(event).await;
                } else if trace.transition == "Deactivated" {
                    let event = observability::AuditEvent::new(
                        observability::AuditEventType::RuleDeactivated,
                        format!("Rule '{}' deactivated", trace.rule_name),
                    )
                    .with_rule(&trace.rule_id, &trace.rule_name);

                    let _ = audit_log.log(event).await;
                }
            }
        }

        // ====================================================================
        // PHASE 5: Tracing & Observability
        // ====================================================================
        batch_span.set_attribute(opentelemetry::KeyValue::new(
            "traces.count",
            traces.len() as i64,
        ));
        batch_span.end();

        Ok(traces)
    }

    /// Process a batch (backward-compatible wrapper)
    ///
    /// This is a convenience wrapper that calls `process_batch_with_tenant(batch, None)`.
    /// Use `process_batch_with_tenant()` directly for multi-tenant scenarios.
    pub async fn process_batch(&mut self, batch: &RecordBatch) -> Result<Vec<EvaluationTrace>> {
        self.process_batch_with_tenant(batch, None).await
    }
}
