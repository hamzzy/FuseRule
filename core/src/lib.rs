//! # FuseRule - High-Performance Rule Engine
//!
//! FuseRule is a high-performance, Arrow-native Complex Event Processing (CEP) engine
//! with SQL-powered rules for real-time data auditing and event processing.
//!
//! ## Quick Start
//!
//! ```no_run
//! use fuse_rule::{RuleEngine, config::FuseRuleConfig};
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
pub mod rule;
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

        // 3. Add Agents - DECOUPLED
        // Agents must be added by the caller (CLI) as Core does not know about specific agents.

        // 4. Add Rules
        for r_cfg in config.rules {
            engine
                .add_rule(Rule {
                    id: r_cfg.id,
                    name: r_cfg.name,
                    predicate: r_cfg.predicate,
                    action: r_cfg.action,
                    window_seconds: r_cfg.window_seconds,
                    version: r_cfg.version,
                    enabled: r_cfg.enabled,
                    description: r_cfg.description,
                    tags: r_cfg.tags,
                })
                .await?;
        }

        Ok(engine)
    }

    pub async fn reload_from_config(&mut self, config: FuseRuleConfig) -> Result<()> {
        info!("🔄 Reloading engine configuration...");

        // 1. Update Agents - DECOUPLED
        // Caller update agents.

        // 2. Update Rules
        let mut new_rules = Vec::new();
        let mut new_window_buffers = HashMap::new();

        for r_cfg in config.rules {
            let rule = Rule {
                id: r_cfg.id,
                name: r_cfg.name,
                predicate: r_cfg.predicate,
                action: r_cfg.action,
                window_seconds: r_cfg.window_seconds,
                version: r_cfg.version,
                enabled: r_cfg.enabled,
                description: r_cfg.description,
                tags: r_cfg.tags,
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

    pub async fn add_rule(&mut self, rule: Rule) -> Result<()> {
        if let Some(secs) = rule.window_seconds {
            self.window_buffers
                .insert(rule.id.clone(), WindowBuffer::new(secs));
        }
        let compiled = self.evaluator.compile(rule, &self.schema)?;
        self.rules.push(compiled);
        Ok(())
    }

    pub async fn update_rule(&mut self, rule_id: &str, new_rule: Rule) -> Result<()> {
        // Find existing rule
        let rule_idx = self.rules.iter().position(|r| r.rule.id == rule_id);
        if rule_idx.is_none() {
            anyhow::bail!("Rule not found: {}", rule_id);
        }

        let rule_idx = rule_idx.unwrap();
        let old_rule = &self.rules[rule_idx].rule;

        // Preserve window buffer if window_seconds unchanged
        let preserve_buffer = old_rule.window_seconds == new_rule.window_seconds;
        let existing_buffer = if preserve_buffer {
            self.window_buffers.remove(rule_id)
        } else {
            None
        };

        // Compile new rule
        let compiled = self.evaluator.compile(new_rule, &self.schema)?;

        // Replace rule
        self.rules[rule_idx] = compiled;

        // Restore or create window buffer
        if let Some(buffer) = existing_buffer {
            self.window_buffers.insert(rule_id.to_string(), buffer);
        } else if let Some(secs) = self.rules[rule_idx].rule.window_seconds {
            self.window_buffers
                .insert(rule_id.to_string(), WindowBuffer::new(secs));
        }

        Ok(())
    }

    pub async fn toggle_rule(&mut self, rule_id: &str, enabled: bool) -> Result<()> {
        let rule_idx = self.rules.iter().position(|r| r.rule.id == rule_id);
        if let Some(idx) = rule_idx {
            self.rules[idx].rule.enabled = enabled;
            Ok(())
        } else {
            anyhow::bail!("Rule not found: {}", rule_id)
        }
    }

    pub async fn process_batch(&mut self, batch: &RecordBatch) -> Result<Vec<EvaluationTrace>> {
        let _start = Instant::now();

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

        // Parallel rule evaluation using tokio::join_all
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
        let mut traces = Vec::new();

        // Process enabled rules and create traces for all rules (disabled ones get None transition)
        let enabled_count = enabled_indices.len();
        let mut enabled_results_iter = results_with_context.into_iter();
        let mut enabled_idx_iter = enabled_indices.into_iter();

        // Record per-rule evaluation duration (approximate by dividing total time)
        // In a more sophisticated implementation, we'd track each rule individually
        let eval_duration = evaluation_start.elapsed();
        let per_rule_duration = if enabled_count > 0 {
            eval_duration.as_secs_f64() / enabled_count as f64
        } else {
            0.0
        };

        for rule in self.rules.iter() {
            if rule.rule.enabled {
                let original_idx = enabled_idx_iter.next().unwrap();
                let (result, context) = enabled_results_iter.next().unwrap();
                let rule = &self.rules[original_idx].rule;

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

                if let RuleTransition::Activated = transition {
                    action_fired = true;
                    crate::metrics::METRICS
                        .activations_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::metrics::METRICS.record_rule_activation(&rule.id);
                    let activation = Activation {
                        rule_id: rule.id.clone(),
                        rule_name: rule.name.clone(),
                        action: rule.action.clone(),
                        context,
                    };

                    // Use async agent queue if available, otherwise execute synchronously
                    if let Some(agent_queue) = &self.agent_queue {
                        if let Some(agent) = self.agents.get(&rule.action) {
                            let circuit_breaker = self.circuit_breakers.get(&rule.action).cloned();

                            // Get DLQ sender from queue
                            let dlq_sender = Some(agent_queue.dlq_sender.clone());

                            let task = AgentTask::new(
                                activation,
                                agent.clone(),
                                3, // max_retries
                                circuit_breaker,
                                dlq_sender,
                            );

                            if let Err(e) = agent_queue.enqueue(task).await {
                                error!("Failed to enqueue agent task: {}", e);
                                agent_status = Some(format!("enqueue_failed: {}", e));
                                crate::metrics::METRICS
                                    .agent_failures
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                agent_status = Some("queued".to_string());
                            }
                        } else {
                            agent_status = Some("agent_not_found".to_string());
                            crate::metrics::METRICS
                                .agent_failures
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    } else {
                        // Fallback to synchronous execution
                        if let Some(agent) = self.agents.get(&rule.action) {
                            match agent.execute(&activation).await {
                                Ok(_) => {
                                    debug!(
                                        "Agent '{}' executed successfully for rule '{}'",
                                        rule.action, rule.id
                                    );
                                    agent_status = Some("success".to_string());
                                }
                                Err(e) => {
                                    error!("Error executing agent '{}': {}", rule.action, e);
                                    agent_status = Some(format!("failed: {}", e));
                                    crate::metrics::METRICS
                                        .agent_failures
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                            }
                        } else {
                            agent_status = Some("agent_not_found".to_string());
                            crate::metrics::METRICS
                                .agent_failures
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
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
                let rule = &rule.rule;
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

        Ok(traces)
    }
}
