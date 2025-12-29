pub mod rule;
pub mod state;
pub mod agent;
pub mod evaluator;
pub mod window;
pub mod config;
pub mod server;

use arrow::record_batch::RecordBatch;
use crate::rule::Rule;
use crate::state::{StateStore, RuleTransition, PredicateResult};
use crate::agent::{Activation, Agent};
use crate::evaluator::{RuleEvaluator, CompiledRuleEdge};
use crate::window::WindowBuffer;
use crate::config::FuseRuleConfig;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

/// A trace of what happened during an evaluation batch (Principle 4: Observability)
#[derive(Debug, Clone, serde::Serialize)]
pub struct EvaluationTrace {
    pub rule_id: String,
    pub rule_name: String,
    pub result: PredicateResult,
    pub transition: String, // "None", "Activated", "Deactivated"
    pub action_fired: bool,
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
        schema: Arc<arrow::datatypes::Schema>
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
        let state = Box::new(crate::state::SledStateStore::new(&config.engine.persistence_path)?);

        let mut engine = Self::new(evaluator, state, Arc::clone(&schema));

        // 3. Add Agents
        for agent_cfg in config.agents {
            match agent_cfg.r#type.as_str() {
                "logger" => {
                    engine.add_agent(agent_cfg.name, Arc::new(crate::agent::LoggerAgent));
                }
                "webhook" => {
                    if let Some(url) = agent_cfg.url {
                        engine.add_agent(agent_cfg.name, Arc::new(crate::agent::WebhookAgent::new(url)));
                    }
                }
                _ => println!("Warning: Unknown agent type '{}'", agent_cfg.r#type),
            }
        }

        // 4. Add Rules
        for r_cfg in config.rules {
            engine.add_rule(Rule {
                id: r_cfg.id,
                name: r_cfg.name,
                predicate: r_cfg.predicate,
                action: r_cfg.action,
                window_seconds: r_cfg.window_seconds,
            }).await?;
        }

        Ok(engine)
    }

    pub fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        Arc::clone(&self.schema)
    }

    pub fn add_agent(&mut self, name: String, agent: Arc<dyn Agent>) {
        self.agents.insert(name, agent);
    }

    pub async fn add_rule(&mut self, rule: Rule) -> Result<()> {
        if let Some(secs) = rule.window_seconds {
            self.window_buffers.insert(rule.id.clone(), WindowBuffer::new(secs));
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

        let results_with_context = self.evaluator.evaluate_batch(batch, &self.rules, &windowed_data).await?;
        println!("  Evaluator returned {} results for {} rules", results_with_context.len(), self.rules.len());
        let mut traces = Vec::new();

        for (i, (result, context)) in results_with_context.into_iter().enumerate() {
            let rule = &self.rules[i].rule;
            let transition = self.state.update_result(&rule.id, result).await?;
            
            if transition != RuleTransition::None {
                println!("  [Engine] Rule '{}' ({}): {:?} -> {:?}", rule.name, rule.id, result, transition);
            }

            let mut action_fired = false;
            if let RuleTransition::Activated = transition {
                let activation = Activation {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    action: rule.action.clone(),
                    context,
                };
                
                if let Some(agent) = self.agents.get(&rule.action) {
                    let agent_clone = Arc::clone(agent);
                    let activation_clone = activation.clone();
                    action_fired = true;
                    tokio::spawn(async move {
                        if let Err(e) = agent_clone.execute(&activation_clone).await {
                            eprintln!("Error executing agent: {}", e);
                        }
                    });
                }
            }

            traces.push(EvaluationTrace {
                rule_id: rule.id.clone(),
                rule_name: rule.name.clone(),
                result,
                transition: match transition {
                    RuleTransition::None => "None".to_string(),
                    RuleTransition::Activated => "Activated".to_string(),
                    RuleTransition::Deactivated => "Deactivated".to_string(),
                },
                action_fired,
            });

            if let Some(buffer) = self.window_buffers.get_mut(&rule.id) {
                buffer.add_batch(batch.clone());
            }
        }

        Ok(traces)
    }
}
