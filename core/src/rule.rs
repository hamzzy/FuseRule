use serde::{Deserialize, Serialize};
use crate::rule_graph::conditional_action::ConditionalAction;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub id: String,
    pub name: String,
    pub predicate: String, // SQL-like expression: "price > 100 AND volume < 50"
    pub action: String,
    pub window_seconds: Option<u64>,
    pub version: u32,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Optional description of what the rule does
    #[serde(default)]
    pub description: Option<String>,
    /// Optional tags for categorizing and filtering rules
    #[serde(default)]
    pub tags: Vec<String>,
    /// NEW: Conditional actions (multiple actions per rule)
    #[serde(default)]
    pub conditional_actions: Vec<ConditionalAction>,
    /// NEW: Downstream rules for DAG execution
    #[serde(default)]
    pub downstream_rules: Vec<String>,
    /// NEW: Flag indicating if this rule uses dynamic predicates
    #[serde(default)]
    pub has_dynamic_predicate: bool,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub rule: Rule,
    // The physical expression will be stored in the evaluator
}
