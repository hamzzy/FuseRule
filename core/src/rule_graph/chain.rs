//! Rule chaining and composition
//!
//! Enables rules to trigger other rules in a directed acyclic graph (DAG).
//!
//! Example workflow:
//! ```text
//! high_price → check_volume → alert_trader
//!           ↘
//!             check_volatility → alert_risk_management
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use anyhow::Result;

/// A rule that can trigger downstream rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainedRule {
    /// Base rule ID
    pub rule_id: String,
    /// Rules to trigger when this rule activates
    pub downstream_rules: Vec<String>,
    /// Conditions for triggering downstream rules
    pub trigger_conditions: HashMap<String, TriggerCondition>,
}

/// Condition for triggering a downstream rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriggerCondition {
    /// Always trigger when parent activates
    Always,
    /// Trigger only if specific field matches value
    FieldEquals { field: String, value: String },
    /// Trigger only if custom expression evaluates to true
    Expression { expr: String },
    /// Trigger after N consecutive activations
    ConsecutiveCount { count: u32 },
}

/// Rule chain configuration
#[derive(Debug, Clone)]
pub struct RuleChain {
    /// Map of rule_id -> ChainedRule
    chains: HashMap<String, ChainedRule>,
    /// Reverse index: downstream_rule_id -> upstream_rule_ids
    reverse_index: HashMap<String, Vec<String>>,
}

impl RuleChain {
    /// Create a new empty rule chain
    pub fn new() -> Self {
        Self {
            chains: HashMap::new(),
            reverse_index: HashMap::new(),
        }
    }

    /// Add a chained rule
    pub fn add_chain(&mut self, chained_rule: ChainedRule) -> Result<()> {
        let rule_id = chained_rule.rule_id.clone();

        // Update reverse index
        for downstream_id in &chained_rule.downstream_rules {
            self.reverse_index
                .entry(downstream_id.clone())
                .or_insert_with(Vec::new)
                .push(rule_id.clone());
        }

        self.chains.insert(rule_id, chained_rule);
        Ok(())
    }

    /// Get downstream rules for a given rule
    pub fn get_downstream(&self, rule_id: &str) -> Vec<String> {
        self.chains
            .get(rule_id)
            .map(|c| c.downstream_rules.clone())
            .unwrap_or_default()
    }

    /// Get upstream rules for a given rule
    pub fn get_upstream(&self, rule_id: &str) -> Vec<String> {
        self.reverse_index
            .get(rule_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get trigger condition for a downstream rule
    pub fn get_trigger_condition(&self, upstream_id: &str, downstream_id: &str) -> Option<&TriggerCondition> {
        self.chains
            .get(upstream_id)
            .and_then(|c| c.trigger_conditions.get(downstream_id))
    }

    /// Validate the chain has no cycles (must be a DAG)
    pub fn validate_dag(&self) -> Result<()> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for rule_id in self.chains.keys() {
            if !visited.contains(rule_id) {
                if self.has_cycle_util(rule_id, &mut visited, &mut rec_stack) {
                    anyhow::bail!("Cycle detected in rule chain starting at '{}'", rule_id);
                }
            }
        }

        Ok(())
    }

    fn has_cycle_util(
        &self,
        rule_id: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        visited.insert(rule_id.to_string());
        rec_stack.insert(rule_id.to_string());

        if let Some(chained) = self.chains.get(rule_id) {
            for downstream_id in &chained.downstream_rules {
                if !visited.contains(downstream_id) {
                    if self.has_cycle_util(downstream_id, visited, rec_stack) {
                        return true;
                    }
                } else if rec_stack.contains(downstream_id) {
                    return true;
                }
            }
        }

        rec_stack.remove(rule_id);
        false
    }

    /// Get all rules in topological order (dependencies before dependents)
    pub fn topological_sort(&self) -> Result<Vec<String>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut all_rules: HashSet<String> = HashSet::new();

        // Collect all rule IDs
        for (rule_id, chained) in &self.chains {
            all_rules.insert(rule_id.clone());
            in_degree.entry(rule_id.clone()).or_insert(0);

            for downstream_id in &chained.downstream_rules {
                all_rules.insert(downstream_id.clone());
                *in_degree.entry(downstream_id.clone()).or_insert(0) += 1;
            }
        }

        // Kahn's algorithm
        let mut queue: Vec<String> = in_degree
            .iter()
            .filter_map(|(id, &deg)| if deg == 0 { Some(id.clone()) } else { None })
            .collect();

        let mut sorted = Vec::new();

        while let Some(rule_id) = queue.pop() {
            sorted.push(rule_id.clone());

            if let Some(chained) = self.chains.get(&rule_id) {
                for downstream_id in &chained.downstream_rules {
                    if let Some(deg) = in_degree.get_mut(downstream_id) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push(downstream_id.clone());
                        }
                    }
                }
            }
        }

        if sorted.len() != all_rules.len() {
            anyhow::bail!("Cycle detected in rule chain (topological sort failed)");
        }

        Ok(sorted)
    }
}

impl Default for RuleChain {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_chain() {
        let mut chain = RuleChain::new();

        chain
            .add_chain(ChainedRule {
                rule_id: "high_price".to_string(),
                downstream_rules: vec!["check_volume".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        chain
            .add_chain(ChainedRule {
                rule_id: "check_volume".to_string(),
                downstream_rules: vec!["alert_trader".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        assert!(chain.validate_dag().is_ok());

        let downstream = chain.get_downstream("high_price");
        assert_eq!(downstream, vec!["check_volume"]);

        let upstream = chain.get_upstream("alert_trader");
        assert_eq!(upstream, vec!["check_volume"]);
    }

    #[test]
    fn test_cycle_detection() {
        let mut chain = RuleChain::new();

        chain
            .add_chain(ChainedRule {
                rule_id: "rule_a".to_string(),
                downstream_rules: vec!["rule_b".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        chain
            .add_chain(ChainedRule {
                rule_id: "rule_b".to_string(),
                downstream_rules: vec!["rule_a".to_string()],
                trigger_conditions: HashMap::new(),
            })
            .unwrap();

        assert!(chain.validate_dag().is_err());
    }

    #[test]
    fn test_topological_sort() {
        let mut chain = RuleChain::new();

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

        let sorted = chain.topological_sort().unwrap();

        // 'a' must come before 'b' and 'c'
        let a_pos = sorted.iter().position(|r| r == "a").unwrap();
        let b_pos = sorted.iter().position(|r| r == "b").unwrap();
        let c_pos = sorted.iter().position(|r| r == "c").unwrap();
        let d_pos = sorted.iter().position(|r| r == "d").unwrap();

        assert!(a_pos < b_pos);
        assert!(a_pos < c_pos);
        assert!(b_pos < d_pos);
        assert!(c_pos < d_pos);
    }
}
