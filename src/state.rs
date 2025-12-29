use std::path::Path;
use sled::Db;
use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PredicateResult {
    True,
    False,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleTransition {
    None,
    Activated, // False -> True
    Deactivated, // True -> False
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn get_last_result(&self, rule_id: &str) -> Result<PredicateResult>;
    async fn update_result(&self, rule_id: &str, current: PredicateResult) -> Result<RuleTransition>;
}

pub struct SledStateStore {
    db: Db,
}

impl SledStateStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }
}

#[async_trait]
impl StateStore for SledStateStore {
    async fn get_last_result(&self, rule_id: &str) -> Result<PredicateResult> {
        let key = format!("rule_state:{}", rule_id);
        let last_bytes = self.db.get(&key)?;
        
        if let Some(bytes) = last_bytes {
            Ok(serde_json::from_slice(&bytes)?)
        } else {
            Ok(PredicateResult::False)
        }
    }

    async fn update_result(&self, rule_id: &str, current: PredicateResult) -> Result<RuleTransition> {
        let last_result = self.get_last_result(rule_id).await?;

        let transition = match (last_result, current) {
            (PredicateResult::False, PredicateResult::True) => RuleTransition::Activated,
            (PredicateResult::True, PredicateResult::False) => RuleTransition::Deactivated,
            _ => RuleTransition::None,
        };

        let key = format!("rule_state:{}", rule_id);
        let current_bytes = serde_json::to_vec(&current)?;
        self.db.insert(key, current_bytes)?;
        
        Ok(transition)
    }
}
