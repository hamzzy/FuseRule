pub mod rule;
pub mod state;
pub mod agent;
pub mod evaluator;
pub mod window;

use arrow::record_batch::RecordBatch;
use crate::rule::Rule;
use crate::state::{EngineState, RuleTransition};
use crate::agent::Activation;
use crate::evaluator::{DataFusionEvaluator, CompiledPhysicalRule};
use crate::window::WindowBuffer;
use anyhow::Result;
use std::path::Path;
use std::collections::HashMap;

pub struct RuleEngine {
    evaluator: DataFusionEvaluator,
    rules: Vec<CompiledPhysicalRule>,
    state: EngineState,
    window_buffers: HashMap<String, WindowBuffer>,
}

impl RuleEngine {
    pub fn new<P: AsRef<Path>>(persistence_path: P) -> Result<Self> {
        Ok(Self {
            evaluator: DataFusionEvaluator::new(),
            rules: Vec::new(),
            state: EngineState::new(persistence_path)?,
            window_buffers: HashMap::new(),
        })
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
        // Collect windowed batches for each rule
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
                activations.push(Activation {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    action: rule.action.clone(),
                    context,
                });
            }

            // Update window buffer with the *current* batch after evaluation (for sliding behavior)
            if let Some(buffer) = self.window_buffers.get_mut(&rule.id) {
                buffer.add_batch(batch.clone());
            }
        }

        Ok(activations)
    }
}
