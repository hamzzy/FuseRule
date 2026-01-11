//! Immutable audit log for compliance and debugging
//!
//! Captures all significant events:
//! - Rule lifecycle (create, update, delete, enable, disable)
//! - Configuration changes
//! - State transitions
//! - Agent executions
//!
//! Events are write-only and persisted to durable storage

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sled::Db;
use std::sync::Arc;
use tracing::{debug, error};
use uuid::Uuid;

/// Type of audit event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditEventType {
    /// Rule was created
    RuleCreated,
    /// Rule was updated
    RuleUpdated,
    /// Rule was deleted
    RuleDeleted,
    /// Rule was enabled
    RuleEnabled,
    /// Rule was disabled
    RuleDisabled,
    /// Configuration was reloaded
    ConfigReloaded,
    /// Rule transitioned to activated state
    RuleActivated,
    /// Rule transitioned to deactivated state
    RuleDeactivated,
    /// Agent execution started
    AgentExecutionStarted,
    /// Agent execution succeeded
    AgentExecutionSucceeded,
    /// Agent execution failed
    AgentExecutionFailed,
    /// State was manually cleared
    StateCleared,
    /// Engine started
    EngineStarted,
    /// Engine stopped
    EngineStopped,
}

/// Immutable audit event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Unique event ID
    pub id: String,
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    /// Event type
    pub event_type: AuditEventType,
    /// Rule ID if applicable
    pub rule_id: Option<String>,
    /// Rule name if applicable
    pub rule_name: Option<String>,
    /// Agent name if applicable
    pub agent_name: Option<String>,
    /// User/actor who triggered the event (if tracked)
    pub actor: Option<String>,
    /// Additional metadata as JSON
    pub metadata: serde_json::Value,
    /// Human-readable description
    pub description: String,
}

impl AuditEvent {
    /// Create a new audit event
    pub fn new(event_type: AuditEventType, description: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            event_type,
            rule_id: None,
            rule_name: None,
            agent_name: None,
            actor: None,
            metadata: serde_json::Value::Null,
            description: description.into(),
        }
    }

    /// Set rule context
    pub fn with_rule(mut self, rule_id: impl Into<String>, rule_name: impl Into<String>) -> Self {
        self.rule_id = Some(rule_id.into());
        self.rule_name = Some(rule_name.into());
        self
    }

    /// Set agent context
    pub fn with_agent(mut self, agent_name: impl Into<String>) -> Self {
        self.agent_name = Some(agent_name.into());
        self
    }

    /// Set actor
    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// Set metadata
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Audit log storage
pub struct AuditLog {
    db: Arc<Db>,
    tree_name: &'static str,
}

impl AuditLog {
    /// Create a new audit log backed by Sled
    pub fn new(db: Arc<Db>) -> Result<Self> {
        Ok(Self {
            db,
            tree_name: "audit_log",
        })
    }

    /// Append an event to the audit log
    pub async fn log(&self, event: AuditEvent) -> Result<()> {
        let tree = self.db.open_tree(self.tree_name)?;

        // Key: timestamp + event_id for chronological ordering
        let key = format!("{}_{}", event.timestamp.timestamp_nanos_opt().unwrap_or(0), event.id);

        let value = serde_json::to_vec(&event)?;

        tree.insert(key.as_bytes(), value)?;

        debug!(
            event_id = %event.id,
            event_type = ?event.event_type,
            rule_id = ?event.rule_id,
            "Audit event logged"
        );

        Ok(())
    }

    /// Query events by time range
    pub async fn query_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<AuditEvent>> {
        let tree = self.db.open_tree(self.tree_name)?;

        let start_key = format!("{}_", start.timestamp_nanos_opt().unwrap_or(0));
        let end_key = format!("{}_", end.timestamp_nanos_opt().unwrap_or(i64::MAX));

        let mut events = Vec::new();

        for item in tree.range(start_key.as_bytes()..end_key.as_bytes()) {
            if events.len() >= limit {
                break;
            }

            match item {
                Ok((_key, value)) => {
                    match serde_json::from_slice::<AuditEvent>(&value) {
                        Ok(event) => events.push(event),
                        Err(e) => {
                            error!(error = %e, "Failed to deserialize audit event");
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to read audit event from storage");
                }
            }
        }

        Ok(events)
    }

    /// Query events by rule ID
    pub async fn query_by_rule_id(&self, rule_id: &str, limit: usize) -> Result<Vec<AuditEvent>> {
        let tree = self.db.open_tree(self.tree_name)?;
        let mut events = Vec::new();

        for item in tree.iter() {
            if events.len() >= limit {
                break;
            }

            if let Ok((_key, value)) = item {
                if let Ok(event) = serde_json::from_slice::<AuditEvent>(&value) {
                    if event.rule_id.as_deref() == Some(rule_id) {
                        events.push(event);
                    }
                }
            }
        }

        // Reverse to get most recent first
        events.reverse();
        Ok(events)
    }

    /// Query events by type
    pub async fn query_by_type(&self, event_type: AuditEventType, limit: usize) -> Result<Vec<AuditEvent>> {
        let tree = self.db.open_tree(self.tree_name)?;
        let mut events = Vec::new();

        for item in tree.iter() {
            if events.len() >= limit {
                break;
            }

            if let Ok((_key, value)) = item {
                if let Ok(event) = serde_json::from_slice::<AuditEvent>(&value) {
                    if event.event_type == event_type {
                        events.push(event);
                    }
                }
            }
        }

        // Reverse to get most recent first
        events.reverse();
        Ok(events)
    }

    /// Get total event count
    pub fn count(&self) -> Result<usize> {
        let tree = self.db.open_tree(self.tree_name)?;
        Ok(tree.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_audit_event_creation() {
        let event = AuditEvent::new(AuditEventType::RuleCreated, "Test rule created")
            .with_rule("test_rule", "Test Rule")
            .with_actor("system");

        assert_eq!(event.event_type, AuditEventType::RuleCreated);
        assert_eq!(event.rule_id, Some("test_rule".to_string()));
        assert_eq!(event.actor, Some("system".to_string()));
    }

    #[tokio::test]
    async fn test_audit_log_append() {
        let dir = tempdir().unwrap();
        let db = Arc::new(sled::open(dir.path()).unwrap());
        let audit_log = AuditLog::new(db).unwrap();

        let event = AuditEvent::new(AuditEventType::RuleCreated, "Test event");

        let result = audit_log.log(event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_audit_log_query_by_rule() {
        let dir = tempdir().unwrap();
        let db = Arc::new(sled::open(dir.path()).unwrap());
        let audit_log = AuditLog::new(db).unwrap();

        // Log multiple events
        audit_log
            .log(
                AuditEvent::new(AuditEventType::RuleCreated, "Event 1")
                    .with_rule("rule_1", "Rule 1"),
            )
            .await
            .unwrap();

        audit_log
            .log(
                AuditEvent::new(AuditEventType::RuleActivated, "Event 2")
                    .with_rule("rule_1", "Rule 1"),
            )
            .await
            .unwrap();

        audit_log
            .log(
                AuditEvent::new(AuditEventType::RuleCreated, "Event 3")
                    .with_rule("rule_2", "Rule 2"),
            )
            .await
            .unwrap();

        // Query events for rule_1
        let events = audit_log.query_by_rule_id("rule_1", 100).await.unwrap();
        assert_eq!(events.len(), 2);
    }
}
