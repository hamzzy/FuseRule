//! Dynamic predicates - load predicates from external sources
//!
//! Enables:
//! - Loading predicates from DB/S3
//! - A/B testing rule versions
//! - Feature flags per tenant
//! - Runtime predicate updates without restarts

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Source for loading dynamic predicates
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PredicateSource {
    /// Static predicate (inline)
    Static {
        predicate: String,
    },

    /// Load from database
    Database {
        connection_string: String,
        table: String,
        predicate_column: String,
        rule_id_column: String,
    },

    /// Load from S3
    S3 {
        bucket: String,
        key: String,
        region: Option<String>,
    },

    /// Load from HTTP endpoint
    Http {
        url: String,
        auth_header: Option<String>,
    },
}

/// Dynamic predicate with versioning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicPredicate {
    /// Rule ID
    pub rule_id: String,
    /// Source for the predicate
    pub source: PredicateSource,
    /// Predicate versions for A/B testing
    pub versions: HashMap<String, String>,
    /// Active version ID
    pub active_version: Option<String>,
    /// Feature flags (tenant_id -> version_id)
    pub feature_flags: HashMap<String, String>,
    /// Refresh interval in seconds
    pub refresh_interval_seconds: Option<u64>,
}

impl DynamicPredicate {
    /// Get the active predicate for a given tenant
    pub fn get_predicate_for_tenant(&self, tenant_id: Option<&str>) -> Option<String> {
        // Check feature flags first
        if let Some(tid) = tenant_id {
            if let Some(version_id) = self.feature_flags.get(tid) {
                if let Some(predicate) = self.versions.get(version_id) {
                    return Some(predicate.clone());
                }
            }
        }

        // Fall back to active version
        if let Some(version_id) = &self.active_version {
            if let Some(predicate) = self.versions.get(version_id) {
                return Some(predicate.clone());
            }
        }

        // Default to first version if exists
        self.versions.values().next().cloned()
    }

    /// Add a new version for A/B testing
    pub fn add_version(&mut self, version_id: String, predicate: String) {
        self.versions.insert(version_id, predicate);
    }

    /// Set feature flag for a tenant
    pub fn set_feature_flag(&mut self, tenant_id: String, version_id: String) {
        self.feature_flags.insert(tenant_id, version_id);
    }

    /// Remove feature flag for a tenant
    pub fn remove_feature_flag(&mut self, tenant_id: &str) {
        self.feature_flags.remove(tenant_id);
    }
}

/// Loader for dynamic predicates
#[derive(Clone)]
pub struct PredicateLoader {
    predicates: Arc<RwLock<HashMap<String, DynamicPredicate>>>,
}

impl PredicateLoader {
    pub fn new() -> Self {
        Self {
            predicates: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a dynamic predicate
    pub async fn register(&self, predicate: DynamicPredicate) {
        let mut predicates = self.predicates.write().await;
        predicates.insert(predicate.rule_id.clone(), predicate);
    }

    /// Get predicate for a rule and tenant
    pub async fn get_predicate(&self, rule_id: &str, tenant_id: Option<&str>) -> Option<String> {
        let predicates = self.predicates.read().await;
        predicates
            .get(rule_id)
            .and_then(|dp| dp.get_predicate_for_tenant(tenant_id))
    }

    /// Reload predicate from source (for periodic refresh)
    pub async fn reload(&self, rule_id: &str) -> Result<()> {
        let mut predicates = self.predicates.write().await;

        if let Some(dynamic_pred) = predicates.get_mut(rule_id) {
            match &dynamic_pred.source {
                PredicateSource::Static { .. } => {
                    // Static predicates don't need reloading
                    Ok(())
                }
                PredicateSource::Database {
                    connection_string,
                    table,
                    predicate_column,
                    rule_id_column,
                } => {
                    self.reload_from_database(
                        rule_id,
                        connection_string,
                        table,
                        predicate_column,
                        rule_id_column,
                    )
                    .await
                }
                PredicateSource::S3 { bucket, key, region } => {
                    self.reload_from_s3(rule_id, bucket, key, region.as_deref())
                        .await
                }
                PredicateSource::Http { url, auth_header } => {
                    self.reload_from_http(rule_id, url, auth_header.as_deref())
                        .await
                }
            }
        } else {
            anyhow::bail!("Predicate not found: {}", rule_id);
        }
    }

    async fn reload_from_s3(
        &self,
        rule_id: &str,
        bucket: &str,
        key: &str,
        region: Option<&str>,
    ) -> Result<()> {
        use aws_sdk_s3::Client;

        // Load AWS config
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(r) = region {
            config_loader = config_loader.region(aws_config::Region::new(r.to_string()));
        }
        let config = config_loader.load().await;
        let client = Client::new(&config);

        // Fetch object from S3
        let response = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch from S3: {}", e))?;

        // Read body
        let body_bytes = response
            .body
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read S3 object body: {}", e))?
            .into_bytes();

        // Parse JSON
        let body: serde_json::Value = serde_json::from_slice(&body_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to parse S3 object as JSON: {}", e))?;

        // Update predicates (same logic as HTTP)
        let mut predicates = self.predicates.write().await;
        if let Some(dynamic_pred) = predicates.get_mut(rule_id) {
            if let Some(versions_obj) = body.get("versions").and_then(|v| v.as_object()) {
                dynamic_pred.versions.clear();
                for (version_id, predicate_val) in versions_obj {
                    if let Some(predicate_str) = predicate_val.as_str() {
                        dynamic_pred.versions.insert(version_id.clone(), predicate_str.to_string());
                    }
                }
            }

            if let Some(active_version) = body.get("active_version").and_then(|v| v.as_str()) {
                dynamic_pred.active_version = Some(active_version.to_string());
            }

            if let Some(flags_obj) = body.get("feature_flags").and_then(|v| v.as_object()) {
                dynamic_pred.feature_flags.clear();
                for (tenant_id, version_id) in flags_obj {
                    if let Some(version_str) = version_id.as_str() {
                        dynamic_pred.feature_flags.insert(tenant_id.clone(), version_str.to_string());
                    }
                }
            }

            tracing::info!(
                rule_id = %rule_id,
                bucket = %bucket,
                key = %key,
                versions = dynamic_pred.versions.len(),
                "Reloaded predicate from S3"
            );
        }

        Ok(())
    }

    async fn reload_from_database(
        &self,
        rule_id: &str,
        connection_string: &str,
        table: &str,
        predicate_column: &str,
        rule_id_column: &str,
    ) -> Result<()> {
        use clickhouse::Client;

        let client = Client::default().with_url(connection_string);

        // Query format: SELECT predicate, version_id FROM table WHERE rule_id = ?
        let query = format!(
            "SELECT {} as predicate, version_id FROM {} WHERE {} = ?",
            predicate_column, table, rule_id_column
        );

        #[derive(serde::Deserialize, clickhouse::Row)]
        struct PredicateRow {
            predicate: String,
            version_id: String,
        }

        let rows: Vec<PredicateRow> = client
            .query(&query)
            .bind(rule_id)
            .fetch_all()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to query database: {}", e))?;

        let mut predicates = self.predicates.write().await;
        if let Some(dynamic_pred) = predicates.get_mut(rule_id) {
            // Clear existing versions and add new ones
            dynamic_pred.versions.clear();
            for row in rows {
                dynamic_pred.versions.insert(row.version_id, row.predicate);
            }

            tracing::info!(
                rule_id = %rule_id,
                versions = dynamic_pred.versions.len(),
                "Reloaded predicate from database"
            );
        }

        Ok(())
    }

    async fn reload_from_http(
        &self,
        rule_id: &str,
        url: &str,
        auth_header: Option<&str>,
    ) -> Result<()> {
        let client = reqwest::Client::new();
        let mut request = client.get(url);

        if let Some(auth) = auth_header {
            request = request.header("Authorization", auth);
        }

        let response = request.send().await?;
        let body: serde_json::Value = response.json().await?;

        // Parse response and update predicate versions
        // Expected format:
        // {
        //   "rule_id": "example_rule",
        //   "versions": {
        //     "v1": "price > 100",
        //     "v2": "price > 200"
        //   },
        //   "active_version": "v1",
        //   "feature_flags": {
        //     "tenant_123": "v2"
        //   }
        // }

        let mut predicates = self.predicates.write().await;
        if let Some(dynamic_pred) = predicates.get_mut(rule_id) {
            // Update versions if present
            if let Some(versions_obj) = body.get("versions").and_then(|v| v.as_object()) {
                dynamic_pred.versions.clear();
                for (version_id, predicate_val) in versions_obj {
                    if let Some(predicate_str) = predicate_val.as_str() {
                        dynamic_pred.versions.insert(version_id.clone(), predicate_str.to_string());
                    }
                }
            }

            // Update active version if present
            if let Some(active_version) = body.get("active_version").and_then(|v| v.as_str()) {
                dynamic_pred.active_version = Some(active_version.to_string());
            }

            // Update feature flags if present
            if let Some(flags_obj) = body.get("feature_flags").and_then(|v| v.as_object()) {
                dynamic_pred.feature_flags.clear();
                for (tenant_id, version_id) in flags_obj {
                    if let Some(version_str) = version_id.as_str() {
                        dynamic_pred.feature_flags.insert(tenant_id.clone(), version_str.to_string());
                    }
                }
            }

            tracing::info!(
                rule_id = %rule_id,
                versions = dynamic_pred.versions.len(),
                "Reloaded predicate from HTTP"
            );
        }

        Ok(())
    }

    /// Start periodic refresh background task
    pub fn start_refresh_task(&self, refresh_interval_seconds: u64) {
        let loader = self.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(refresh_interval_seconds));

            loop {
                interval.tick().await;

                let predicates = loader.predicates.read().await;
                let rule_ids: Vec<String> = predicates.keys().cloned().collect();
                drop(predicates);

                for rule_id in rule_ids {
                    if let Err(e) = loader.reload(&rule_id).await {
                        tracing::warn!("Failed to reload predicate for {}: {}", rule_id, e);
                    }
                }
            }
        });
    }
}

impl Default for PredicateLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_predicate_versions() {
        let mut dp = DynamicPredicate {
            rule_id: "test_rule".to_string(),
            source: PredicateSource::Static {
                predicate: "price > 100".to_string(),
            },
            versions: HashMap::new(),
            active_version: None,
            feature_flags: HashMap::new(),
            refresh_interval_seconds: None,
        };

        dp.add_version("v1".to_string(), "price > 100".to_string());
        dp.add_version("v2".to_string(), "price > 200".to_string());
        dp.active_version = Some("v1".to_string());

        // Default tenant gets active version
        assert_eq!(
            dp.get_predicate_for_tenant(None),
            Some("price > 100".to_string())
        );

        // Tenant with feature flag gets their version
        dp.set_feature_flag("tenant_123".to_string(), "v2".to_string());
        assert_eq!(
            dp.get_predicate_for_tenant(Some("tenant_123")),
            Some("price > 200".to_string())
        );

        // Other tenants still get active version
        assert_eq!(
            dp.get_predicate_for_tenant(Some("tenant_456")),
            Some("price > 100".to_string())
        );
    }

    #[tokio::test]
    async fn test_predicate_loader() {
        let loader = PredicateLoader::new();

        let mut dp = DynamicPredicate {
            rule_id: "test_rule".to_string(),
            source: PredicateSource::Static {
                predicate: "price > 100".to_string(),
            },
            versions: HashMap::new(),
            active_version: Some("v1".to_string()),
            feature_flags: HashMap::new(),
            refresh_interval_seconds: None,
        };

        dp.add_version("v1".to_string(), "price > 100".to_string());
        loader.register(dp).await;

        let predicate = loader.get_predicate("test_rule", None).await;
        assert_eq!(predicate, Some("price > 100".to_string()));
    }
}
