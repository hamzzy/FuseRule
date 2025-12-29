use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct Activation {
    pub rule_id: String,
    pub rule_name: String,
    pub action: String,
    pub context: Option<RecordBatch>,
}

#[async_trait]
pub trait Agent: Send + Sync {
    fn name(&self) -> &str;
    async fn execute(&self, activation: &Activation) -> anyhow::Result<()>;
}

pub struct LoggerAgent;

#[async_trait]
impl Agent for LoggerAgent {
    fn name(&self) -> &str {
        "logger"
    }

    async fn execute(&self, activation: &Activation) -> anyhow::Result<()> {
        let rows = activation.context.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        println!("[Agent: {}] Firing action '{}' for rule '{}' ({}) - Context: {} rows matched", 
            self.name(), activation.action, activation.rule_name, activation.rule_id, rows);
        Ok(())
    }
}

pub struct WebhookAgent {
    pub url: String,
    client: reqwest::Client,
}

impl WebhookAgent {
    pub fn new(url: String) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Agent for WebhookAgent {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn execute(&self, activation: &Activation) -> anyhow::Result<()> {
        let payload = serde_json::json!({
            "rule_id": activation.rule_id,
            "rule_name": activation.rule_name,
            "action": activation.action,
            "matched_rows": activation.context.as_ref().map(|b| b.num_rows()).unwrap_or(0),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        println!("[Agent: webhook] Sending POST to {}...", self.url);
        
        // In a real production system, we might want to handle retries here
        self.client.post(&self.url)
            .json(&payload)
            .send()
            .await?;
            
        Ok(())
    }
}
