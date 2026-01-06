use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sled::Db;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PredicateResult {
    True,
    False,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StateEntry {
    result: PredicateResult,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleTransition {
    None,
    Activated,   // False -> True
    Deactivated, // True -> False
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn get_last_result(&self, rule_id: &str) -> Result<PredicateResult>;
    async fn update_result(
        &self,
        rule_id: &str,
        current: PredicateResult,
    ) -> Result<RuleTransition>;
    async fn cleanup_expired(&self, rule_id: &str, ttl_seconds: u64) -> Result<bool>;
    async fn get_last_transition_time(&self, rule_id: &str) -> Result<Option<DateTime<Utc>>>;
}

pub struct SledStateStore {
    db: Db,
}

impl SledStateStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Background task to clean up expired state entries
    pub fn start_cleanup_task(&self, rules_ttl: std::collections::HashMap<String, u64>) {
        let db = self.db.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Run every minute
            loop {
                interval.tick().await;
                for (rule_id, ttl_seconds) in &rules_ttl {
                    if let Err(e) = Self::cleanup_expired_static(&db, rule_id, *ttl_seconds) {
                        tracing::warn!(error = %e, rule_id = %rule_id, "Failed to cleanup expired state");
                    }
                }
            }
        });
    }

    fn cleanup_expired_static(db: &Db, rule_id: &str, ttl_seconds: u64) -> Result<bool> {
        let key = format!("rule_state:{}", rule_id);
        if let Some(bytes) = db.get(&key)? {
            if let Ok(entry) = serde_json::from_slice::<StateEntry>(&bytes) {
                let age = Utc::now().signed_duration_since(entry.timestamp);
                if age.num_seconds() > ttl_seconds as i64 {
                    db.remove(&key)?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

#[async_trait]
impl StateStore for SledStateStore {
    async fn get_last_result(&self, rule_id: &str) -> Result<PredicateResult> {
        let key = format!("rule_state:{}", rule_id);
        let last_bytes = self.db.get(&key)?;

        if let Some(bytes) = last_bytes {
            // Try to deserialize as StateEntry (new format with timestamp)
            if let Ok(entry) = serde_json::from_slice::<StateEntry>(&bytes) {
                Ok(entry.result)
            } else {
                // Fallback to old format (just PredicateResult)
                Ok(serde_json::from_slice(&bytes)?)
            }
        } else {
            Ok(PredicateResult::False)
        }
    }

    async fn update_result(
        &self,
        rule_id: &str,
        current: PredicateResult,
    ) -> Result<RuleTransition> {
        let last_result = self.get_last_result(rule_id).await?;

        let transition = match (last_result, current) {
            (PredicateResult::False, PredicateResult::True) => RuleTransition::Activated,
            (PredicateResult::True, PredicateResult::False) => RuleTransition::Deactivated,
            _ => RuleTransition::None,
        };

        let key = format!("rule_state:{}", rule_id);
        let entry = StateEntry {
            result: current,
            timestamp: Utc::now(),
        };
        let current_bytes = serde_json::to_vec(&entry)?;
        self.db.insert(key, current_bytes)?;

        Ok(transition)
    }

    async fn cleanup_expired(&self, rule_id: &str, ttl_seconds: u64) -> Result<bool> {
        Ok(Self::cleanup_expired_static(
            &self.db,
            rule_id,
            ttl_seconds,
        )?)
    }

    async fn get_last_transition_time(&self, rule_id: &str) -> Result<Option<DateTime<Utc>>> {
        let key = format!("rule_state:{}", rule_id);
        if let Some(bytes) = self.db.get(&key)? {
            if let Ok(entry) = serde_json::from_slice::<StateEntry>(&bytes) {
                return Ok(Some(entry.timestamp));
            }
        }
        Ok(None)
    }
}
