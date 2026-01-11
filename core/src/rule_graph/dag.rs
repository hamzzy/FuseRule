//! DAG executor - execute rules in topological order
//!
//! Enables complex rule workflows by executing rules as a directed acyclic graph

use crate::rule_graph::chain::{RuleChain, TriggerCondition};
use anyhow::Result;
use std::collections::{HashMap, HashSet};

/// Execution plan for a rule DAG
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Rules in topological order (dependencies first)
    pub execution_order: Vec<String>,
    /// Map of rule_id -> level (for parallel execution)
    pub levels: HashMap<String, usize>,
}

/// Rule DAG structure
#[derive(Debug, Clone)]
pub struct RuleDAG {
    chain: RuleChain,
}

impl RuleDAG {
    pub fn new(chain: RuleChain) -> Result<Self> {
        // Validate DAG on construction
        chain.validate_dag()?;
        Ok(Self { chain })
    }

    /// Get execution plan
    pub fn get_execution_plan(&self) -> Result<ExecutionPlan> {
        let execution_order = self.chain.topological_sort()?;
        let levels = self.compute_levels(&execution_order);

        Ok(ExecutionPlan {
            execution_order,
            levels,
        })
    }

    fn compute_levels(&self, execution_order: &[String]) -> HashMap<String, usize> {
        let mut levels: HashMap<String, usize> = HashMap::new();

        for rule_id in execution_order {
            let upstream = self.chain.get_upstream(rule_id);

            if upstream.is_empty() {
                // Root node
                levels.insert(rule_id.clone(), 0);
            } else {
                // Level is max(upstream levels) + 1
                let max_upstream_level = upstream
                    .iter()
                    .filter_map(|up_id| levels.get(up_id))
                    .max()
                    .copied()
                    .unwrap_or(0);

                levels.insert(rule_id.clone(), max_upstream_level + 1);
            }
        }

        levels
    }

    /// Get rules that can be executed in parallel at each level
    pub fn get_parallel_batches(&self) -> Result<Vec<Vec<String>>> {
        let plan = self.get_execution_plan()?;

        let mut level_to_rules: HashMap<usize, Vec<String>> = HashMap::new();

        for (rule_id, level) in plan.levels {
            level_to_rules
                .entry(level)
                .or_insert_with(Vec::new)
                .push(rule_id);
        }

        let max_level = level_to_rules.keys().max().copied().unwrap_or(0);

        let mut batches = Vec::new();
        for level in 0..=max_level {
            if let Some(rules) = level_to_rules.get(&level) {
                batches.push(rules.clone());
            }
        }

        Ok(batches)
    }
}

/// Executor for rule DAGs
pub struct DAGExecutor {
    dag: RuleDAG,
}

impl DAGExecutor {
    pub fn new(dag: RuleDAG) -> Self {
        Self { dag }
    }

    /// Determine which rules should be triggered based on activation
    pub fn get_triggered_rules(
        &self,
        activated_rule_id: &str,
        context: &HashMap<String, serde_json::Value>,
    ) -> Vec<String> {
        let downstream = self.dag.chain.get_downstream(activated_rule_id);

        downstream
            .into_iter()
            .filter(|downstream_id| {
                if let Some(condition) = self
                    .dag
                    .chain
                    .get_trigger_condition(activated_rule_id, downstream_id)
                {
                    self.evaluate_trigger_condition(condition, context)
                } else {
                    // No condition means always trigger
                    true
                }
            })
            .collect()
    }

    fn evaluate_trigger_condition(
        &self,
        condition: &TriggerCondition,
        context: &HashMap<String, serde_json::Value>,
    ) -> bool {
        match condition {
            TriggerCondition::Always => true,

            TriggerCondition::FieldEquals { field, value } => {
                context.get(field).map_or(false, |v| {
                    if let serde_json::Value::String(s) = v {
                        s == value
                    } else {
                        false
                    }
                })
            }

            TriggerCondition::Expression { expr } => {
                // Simple expression evaluation
                // This could be enhanced with a proper expression engine
                self.evaluate_simple_expression(expr, context)
            }

            TriggerCondition::ConsecutiveCount { .. } => {
                // This requires stateful tracking
                // For now, default to true
                // TODO: Implement consecutive count tracking
                true
            }
        }
    }

    fn evaluate_simple_expression(&self, _expr: &str, _context: &HashMap<String, serde_json::Value>) -> bool {
        // TODO: Implement expression evaluation
        // For now, always return true
        true
    }

    /// Get all rules that have no dependencies (entry points)
    pub fn get_entry_points(&self) -> Vec<String> {
        let plan = self.dag.get_execution_plan().unwrap();

        plan.levels
            .iter()
            .filter_map(|(rule_id, level)| {
                if *level == 0 {
                    Some(rule_id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get all leaf rules (no downstream dependencies)
    pub fn get_leaf_rules(&self) -> Vec<String> {
        let plan = self.dag.get_execution_plan().unwrap();
        let all_rules: HashSet<String> = plan.execution_order.iter().cloned().collect();

        all_rules
            .into_iter()
            .filter(|rule_id| self.dag.chain.get_downstream(rule_id).is_empty())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule_graph::chain::ChainedRule;

    #[test]
    fn test_execution_plan() {
        let mut chain = RuleChain::new();

        chain
            .add_chain(ChainedRule {
                rule_id: "a".to_string(),
                downstream_rules: vec!["b".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        chain
            .add_chain(ChainedRule {
                rule_id: "b".to_string(),
                downstream_rules: vec!["c".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        let dag = RuleDAG::new(chain).unwrap();
        let plan = dag.get_execution_plan().unwrap();

        // Check levels
        assert_eq!(plan.levels.get("a"), Some(&0));
        assert_eq!(plan.levels.get("b"), Some(&1));
        assert_eq!(plan.levels.get("c"), Some(&2));
    }

    #[test]
    fn test_parallel_batches() {
        let mut chain = RuleChain::new();

        // Diamond pattern: a -> b,c -> d
        chain
            .add_chain(ChainedRule {
                rule_id: "a".to_string(),
                downstream_rules: vec!["b".to_string(), "c".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        chain
            .add_chain(ChainedRule {
                rule_id: "b".to_string(),
                downstream_rules: vec!["d".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        chain
            .add_chain(ChainedRule {
                rule_id: "c".to_string(),
                downstream_rules: vec!["d".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        let dag = RuleDAG::new(chain).unwrap();
        let batches = dag.get_parallel_batches().unwrap();

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0], vec!["a"]);
        assert!(batches[1].contains(&"b".to_string()));
        assert!(batches[1].contains(&"c".to_string()));
        assert_eq!(batches[2], vec!["d"]);
    }

    #[test]
    fn test_entry_and_leaf_rules() {
        let mut chain = RuleChain::new();

        chain
            .add_chain(ChainedRule {
                rule_id: "a".to_string(),
                downstream_rules: vec!["b".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        chain
            .add_chain(ChainedRule {
                rule_id: "b".to_string(),
                downstream_rules: vec!["c".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        let dag = RuleDAG::new(chain).unwrap();
        let executor = DAGExecutor::new(dag);

        let entry_points = executor.get_entry_points();
        assert_eq!(entry_points, vec!["a"]);

        let leaf_rules = executor.get_leaf_rules();
        assert_eq!(leaf_rules, vec!["c"]);
    }
}
