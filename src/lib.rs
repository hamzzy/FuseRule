pub mod agent;
pub mod config;
pub mod evaluator;
pub mod metrics;
pub mod rule;
pub mod server;
pub mod state;
pub mod window;

use crate::agent::{Activation, Agent};
use crate::config::FuseRuleConfig;
use crate::evaluator::{CompiledRuleEdge, RuleEvaluator};
use crate::rule::Rule;
use crate::state::{PredicateResult, RuleTransition, StateStore};
use crate::window::WindowBuffer;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// A trace of what happened during an evaluation batch (Principle 4: Observability)
#[derive(Debug, Clone, serde::Serialize)]
pub struct EvaluationTrace {
    pub rule_id: String,
    pub rule_name: String,
    pub rule_version: u32,
    pub result: PredicateResult,
    pub transition: String, // "None", "Activated", "Deactivated"
    pub action_fired: bool,
    pub agent_status: Option<String>,
}

pub struct RuleEngine {
    evaluator: Box<dyn RuleEvaluator>,
    state: Box<dyn StateStore>,
    rules: Vec<CompiledRuleEdge>,
    window_buffers: HashMap<String, WindowBuffer>,
    agents: HashMap<String, Arc<dyn Agent>>,
    schema: Arc<arrow::datatypes::Schema>,
}

impl RuleEngine {
    pub fn new(
        evaluator: Box<dyn RuleEvaluator>,
        state: Box<dyn StateStore>,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Self {
        Self {
            evaluator,
            state,
            rules: Vec::new(),
            window_buffers: HashMap::new(),
            agents: HashMap::new(),
            schema,
        }
    }

    pub async fn from_config(config: FuseRuleConfig) -> Result<Self> {
        // 1. Build Schema
        let mut fields = Vec::new();
        for f in config.schema {
            let dt = match f.data_type.as_str() {
                "int32" => arrow::datatypes::DataType::Int32,
                "float64" => arrow::datatypes::DataType::Float64,
                "bool" => arrow::datatypes::DataType::Boolean,
                _ => arrow::datatypes::DataType::Utf8,
            };
            fields.push(arrow::datatypes::Field::new(f.name, dt, true));
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        // 2. Build Components (Edges)
        let evaluator = Box::new(crate::evaluator::DataFusionEvaluator::new());
        let state = Box::new(crate::state::SledStateStore::new(
            &config.engine.persistence_path,
        )?);

        let mut engine = Self::new(evaluator, state, Arc::clone(&schema));

        // 3. Add Agents
        for agent_cfg in config.agents {
            match agent_cfg.r#type.as_str() {
                "logger" => {
                    engine.add_agent(agent_cfg.name, Arc::new(crate::agent::LoggerAgent));
                }
                "webhook" => {
                    if let Some(url) = agent_cfg.url {
                        engine.add_agent(
                            agent_cfg.name,
                            Arc::new(crate::agent::WebhookAgent::new(url)),
                        );
                    }
                }
                _ => println!("Warning: Unknown agent type '{}'", agent_cfg.r#type),
            }
        }

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
                })
                .await?;
        }

        Ok(engine)
    }

    pub async fn reload_from_config(&mut self, config: FuseRuleConfig) -> Result<()> {
        info!("🔄 Reloading engine configuration...");

        // 1. Update Agents
        let mut new_agents = HashMap::new();
        for agent_cfg in config.agents {
            match agent_cfg.r#type.as_str() {
                "logger" => {
                    new_agents.insert(
                        agent_cfg.name,
                        Arc::new(crate::agent::LoggerAgent) as Arc<dyn Agent>,
                    );
                }
                "webhook" => {
                    if let Some(url) = agent_cfg.url {
                        new_agents.insert(
                            agent_cfg.name,
                            Arc::new(crate::agent::WebhookAgent::new(url)) as Arc<dyn Agent>,
                        );
                    }
                }
                _ => warn!("Unknown agent type '{}' during reload", agent_cfg.r#type),
            }
        }
        self.agents = new_agents;

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

    pub async fn process_batch(&mut self, batch: &RecordBatch) -> Result<Vec<EvaluationTrace>> {
        let mut windowed_data = Vec::with_capacity(self.rules.len());
        for rule in &self.rules {
            if let Some(buffer) = self.window_buffers.get(&rule.rule.id) {
                windowed_data.push(buffer.get_batches());
            } else {
                windowed_data.push(vec![]);
            }
        }

        let results_with_context = self
            .evaluator
            .evaluate_batch(batch, &self.rules, &windowed_data)
            .await?;
        crate::metrics::METRICS
            .batches_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut traces = Vec::new();

        for (i, (result, context)) in results_with_context.into_iter().enumerate() {
            let rule = &self.rules[i].rule;
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
                let activation = Activation {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    action: rule.action.clone(),
                    context,
                };

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

            traces.push(EvaluationTrace {
                rule_id: rule.id.clone(),
                rule_name: rule.name.clone(),
                rule_version: rule.version,
                result,
                transition: match transition {
                    RuleTransition::None => "None".to_string(),
                    RuleTransition::Activated => "Activated".to_string(),
                    RuleTransition::Deactivated => "Deactivated".to_string(),
                },
                action_fired,
                agent_status,
            });

            if let Some(buffer) = self.window_buffers.get_mut(&rule.id) {
                buffer.add_batch(batch.clone());
            }
        }

        Ok(traces)
    }
}
