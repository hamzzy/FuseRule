use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub id: String,
    pub name: String,
    pub predicate: String, // SQL-like expression: "price > 100 AND volume < 50"
    pub action: String,
    pub window_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub rule: Rule,
    // The physical expression will be stored in the evaluator
}
