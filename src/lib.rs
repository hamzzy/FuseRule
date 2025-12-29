pub mod rule;
pub mod state;
pub mod agent;
pub mod evaluator;
pub mod window;
pub mod config;

use arrow::record_batch::RecordBatch;
use crate::rule::Rule;
use crate::state::{EngineState, RuleTransition};
use crate::agent::{Activation, Agent};
use crate::evaluator::{DataFusionEvaluator, CompiledPhysicalRule};
use crate::window::WindowBuffer;
use crate::config::FuseRuleConfig;
use anyhow::Result;
use std::path::Path;
use std::collections::HashMap;
use std::sync::Arc;

pub struct RuleEngine {
    evaluator: DataFusionEvaluator,
    rules: Vec<CompiledPhysicalRule>,
    state: EngineState,
    window_buffers: HashMap<String, WindowBuffer>,
    agents: HashMap<String, Arc<dyn Agent>>,
}

impl RuleEngine {
    pub fn new<P: AsRef<Path>>(persistence_path: P) -> Result<Self> {
        Ok(Self {
            evaluator: DataFusionEvaluator::new(),
            rules: Vec::new(),
            state: EngineState::new(persistence_path)?,
            window_buffers: HashMap::new(),
            agents: HashMap::new(),
        })
    }

    pub fn from_config(config: FuseRuleConfig) -> Result<Self> {
        let mut engine = Self::new(&config.engine.persistence_path)?;
        
        // Add Agents
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

        Ok(engine)
    }

    pub fn add_agent(&mut self, name: String, agent: Arc<dyn Agent>) {
        self.agents.insert(name, agent);
    }

    pub fn add_rule(&mut self, rule: Rule, schema: &arrow::datatypes::Schema) -> Result<()> {
        if let Some(secs) = rule.window_seconds {
            self.window_buffers.insert(rule.id.clone(), WindowBuffer::new(secs));
        }
        let compiled = self.evaluator.compile(rule, schema)?;
        self.rules.push(compiled);
        Ok(())
    }

    pub async fn process_batch(&mut self, batch: &RecordBatch) -> Result<Vec<Activation>> {
        let mut windowed_data = Vec::with_capacity(self.rules.len());
        for rule in &self.rules {
            if let Some(buffer) = self.window_buffers.get(&rule.rule.id) {
                windowed_data.push(buffer.get_batches());
            } else {
                windowed_data.push(vec![]);
            }
        }

        let results_with_context = self.evaluator.evaluate_batch(batch, &self.rules, &windowed_data).await?;
        let mut activations = Vec::new();

        for (i, (result, context)) in results_with_context.into_iter().enumerate() {
            let rule = &self.rules[i].rule;
            let transition = self.state.update_rule(&rule.id, result)?;

            if let RuleTransition::Activated = transition {
                let activation = Activation {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    action: rule.action.clone(),
                    context,
                };
                
                // Fire agents mapped to this action
                if let Some(agent) = self.agents.get(&rule.action) {
                    let agent_clone = Arc::clone(agent);
                    let activation_clone = activation.clone();
                    tokio::spawn(async move {
                        if let Err(e) = agent_clone.execute(&activation_clone).await {
                            eprintln!("Error executing agent: {}", e);
                        }
                    });
                }

                activations.push(activation);
            }

            if let Some(buffer) = self.window_buffers.get_mut(&rule.id) {
                buffer.add_batch(batch.clone());
            }
        }

        Ok(activations)
    }
}
