pub mod rule;
pub mod agent;
pub mod evaluator;

use arrow::record_batch::RecordBatch;
use crate::rule::Rule;
use crate::agent::Activation;
use crate::evaluator::{DataFusionEvaluator, CompiledPhysicalRule, PredicateResult};
use anyhow::Result;
use std::collections::HashMap;

pub struct RuleEngine {
    evaluator: DataFusionEvaluator,
    rules: Vec<CompiledPhysicalRule>,
    last_results: HashMap<String, PredicateResult>,
}

impl RuleEngine {
    pub fn new() -> Result<Self> {
        Ok(Self {
            evaluator: DataFusionEvaluator::new(),
            rules: Vec::new(),
            last_results: HashMap::new(),
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
            let last_result = self.last_results.get(&rule.id).copied().unwrap_or(PredicateResult::False);
            
            if last_result == PredicateResult::False && result == PredicateResult::True {
                activations.push(Activation {
                    rule_id: rule.id.clone(),
                    rule_name: rule.name.clone(),
                    action: rule.action.clone(),
                    context,
                });
            }
            
            self.last_results.insert(rule.id.clone(), result);
        }

        Ok(activations)
    }
}
