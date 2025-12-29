pub mod rule;
pub mod state;
pub mod agent;
pub mod evaluator;

use arrow::record_batch::RecordBatch;
use crate::rule::Rule;
use crate::state::{EngineState, RuleTransition};
use crate::agent::Activation;
use crate::evaluator::{DataFusionEvaluator, CompiledPhysicalRule};
use anyhow::Result;
use std::path::Path;

pub struct RuleEngine {
    evaluator: DataFusionEvaluator,
    rules: Vec<CompiledPhysicalRule>,
    state: EngineState,
}

impl RuleEngine {
    pub fn new<P: AsRef<Path>>(persistence_path: P) -> Result<Self> {
        Ok(Self {
            evaluator: DataFusionEvaluator::new(),
            rules: Vec::new(),
            state: EngineState::new(persistence_path)?,
        })
    }

    pub fn add_rule(&mut self, rule: Rule, schema: &arrow::datatypes::Schema) -> Result<()> {
        let compiled = self.evaluator.compile(rule, schema)?;
        self.rules.push(compiled);
        Ok(())
    }

    pub async fn process_batch(&mut self, batch: &RecordBatch) -> Result<Vec<Activation>> {
        let results_with_context = self.evaluator.evaluate_batch(batch, &self.rules).await?;
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
        }

        Ok(activations)
    }
}
