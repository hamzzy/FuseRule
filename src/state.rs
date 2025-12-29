use std::path::Path;
use sled::Db;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PredicateResult {
    True,
    False,
}

pub struct EngineState {
    db: Db,
}

pub enum RuleTransition {
    None,
    Activated, // False -> True
    Deactivated, // True -> False
}

impl EngineState {
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    pub fn update_rule(&mut self, rule_id: &str, current: PredicateResult) -> anyhow::Result<RuleTransition> {
        let key = format!("rule_state:{}", rule_id);
        let last_bytes = self.db.get(&key)?;
        
        let last_result = if let Some(bytes) = last_bytes {
            serde_json::from_slice(&bytes)?
        } else {
            PredicateResult::False
        };

        let transition = match (last_result, current) {
            (PredicateResult::False, PredicateResult::True) => RuleTransition::Activated,
            (PredicateResult::True, PredicateResult::False) => RuleTransition::Deactivated,
            _ => RuleTransition::None,
        };

        let current_bytes = serde_json::to_vec(&current)?;
        self.db.insert(key, current_bytes)?;
        
        Ok(transition)
    }
}
