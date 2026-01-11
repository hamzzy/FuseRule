//! Integration tests for observability features

#[cfg(test)]
mod tests {
    use crate::observability::{AuditEvent, AuditEventType, AuditLog};
    use chrono::{Duration, Utc};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_audit_log_lifecycle() {
        let dir = tempdir().unwrap();
        let db = Arc::new(sled::open(dir.path()).unwrap());
        let audit_log = AuditLog::new(db).unwrap();

        // Create rule
        let event = AuditEvent::new(AuditEventType::RuleCreated, "Test rule created")
            .with_rule("test_rule", "Test Rule")
            .with_actor("test_user");

        audit_log.log(event).await.unwrap();

        // Update rule
        let event = AuditEvent::new(AuditEventType::RuleUpdated, "Test rule updated")
            .with_rule("test_rule", "Test Rule");

        audit_log.log(event).await.unwrap();

        // Enable rule
        let event = AuditEvent::new(AuditEventType::RuleEnabled, "Test rule enabled")
            .with_rule("test_rule", "Test Rule");

        audit_log.log(event).await.unwrap();

        // Query by rule ID
        let events = audit_log.query_by_rule_id("test_rule", 100).await.unwrap();
        assert_eq!(events.len(), 3);

        // Query by type
        let created_events = audit_log
            .query_by_type(AuditEventType::RuleCreated, 100)
            .await
            .unwrap();
        assert_eq!(created_events.len(), 1);
    }

    #[tokio::test]
    async fn test_audit_log_time_range_query() {
        let dir = tempdir().unwrap();
        let db = Arc::new(sled::open(dir.path()).unwrap());
        let audit_log = AuditLog::new(db).unwrap();

        let now = Utc::now();

        // Log events
        for i in 0..5 {
            let event = AuditEvent::new(
                AuditEventType::RuleActivated,
                format!("Rule {} activated", i),
            )
            .with_rule(&format!("rule_{}", i), &format!("Rule {}", i));

            audit_log.log(event).await.unwrap();
        }

        // Query events from last hour
        let start = now - Duration::hours(1);
        let end = now + Duration::hours(1);
        let events = audit_log.query_by_time_range(start, end, 100).await.unwrap();

        assert!(events.len() >= 5);
    }

    #[tokio::test]
    async fn test_audit_log_metadata() {
        let dir = tempdir().unwrap();
        let db = Arc::new(sled::open(dir.path()).unwrap());
        let audit_log = AuditLog::new(db).unwrap();

        let metadata = serde_json::json!({
            "predicate": "price > 100",
            "action": "webhook",
            "version": 1
        });

        let event = AuditEvent::new(AuditEventType::RuleCreated, "Rule with metadata")
            .with_rule("test_rule", "Test Rule")
            .with_metadata(metadata.clone());

        audit_log.log(event).await.unwrap();

        let events = audit_log.query_by_rule_id("test_rule", 1).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].metadata, metadata);
    }
}
