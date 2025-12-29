use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tracing::{debug, info};

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
        let rows = activation
            .context
            .as_ref()
            .map(|b| b.num_rows())
            .unwrap_or(0);
        
        // Log matched rows data for debugging
        if let Some(batch) = &activation.context {
            let field_names: Vec<String> = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
            debug!(
                agent = self.name(),
                rule_id = %activation.rule_id,
                matched_rows = rows,
                columns = ?field_names,
                "Agent firing with matched data"
            );
        }
        
        info!(
            agent = self.name(),
            action = %activation.action,
            rule_name = %activation.rule_name,
            rule_id = %activation.rule_id,
            matched_rows = rows,
            "Agent firing action"
        );
        Ok(())
    }
}

pub struct WebhookAgent {
    pub url: String,
    client: reqwest::Client,
}

impl WebhookAgent {
    pub fn new(url: String) -> Self {
        // Use connection pooling with proper configuration
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(10)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");
        
        Self {
            url,
            client,
        }
    }
}

#[async_trait]
impl Agent for WebhookAgent {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn execute(&self, activation: &Activation) -> anyhow::Result<()> {
        // Build rich payload with matched rows data
        let mut payload = serde_json::json!({
            "rule_id": activation.rule_id,
            "rule_name": activation.rule_name,
            "action": activation.action,
            "matched_rows": activation.context.as_ref().map(|b| b.num_rows()).unwrap_or(0),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        // Add matched rows data if available (rich context)
        if let Some(batch) = &activation.context {
            // Convert RecordBatch to JSON for rich context
            let schema = batch.schema();
            let mut rows = Vec::new();
            for row_idx in 0..batch.num_rows() {
                let mut row = serde_json::Map::new();
                for col_idx in 0..batch.num_columns() {
                    let field = schema.field(col_idx);
                    let array = batch.column(col_idx);
                    // Convert array value to JSON (simplified - handles common types)
                    let value = match array.data_type() {
                        arrow::datatypes::DataType::Int32 => {
                            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int32Array>() {
                                if !arr.is_null(row_idx) {
                                    serde_json::Value::Number(arr.value(row_idx).into())
                                } else {
                                    serde_json::Value::Null
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        arrow::datatypes::DataType::Int64 => {
                            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
                                if !arr.is_null(row_idx) {
                                    serde_json::Value::Number(arr.value(row_idx).into())
                                } else {
                                    serde_json::Value::Null
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        arrow::datatypes::DataType::Float64 => {
                            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
                                if !arr.is_null(row_idx) {
                                    serde_json::Value::Number(
                                        serde_json::Number::from_f64(arr.value(row_idx))
                                            .unwrap_or_else(|| serde_json::Number::from(0))
                                    )
                                } else {
                                    serde_json::Value::Null
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        arrow::datatypes::DataType::Boolean => {
                            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::BooleanArray>() {
                                if !arr.is_null(row_idx) {
                                    serde_json::Value::Bool(arr.value(row_idx))
                                } else {
                                    serde_json::Value::Null
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        arrow::datatypes::DataType::Utf8 => {
                            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
                                if !arr.is_null(row_idx) {
                                    serde_json::Value::String(arr.value(row_idx).to_string())
                                } else {
                                    serde_json::Value::Null
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        _ => serde_json::Value::Null,
                    };
                    row.insert(field.name().clone(), value);
                }
                rows.push(serde_json::Value::Object(row));
            }
            payload.as_object_mut()
                .unwrap()
                .insert("matched_data".to_string(), serde_json::Value::Array(rows));
        }

        debug!(url = %self.url, rule_id = %activation.rule_id, "Sending webhook with rich context");

        // In a real production system, we might want to handle retries here
        self.client.post(&self.url).json(&payload).send().await?;

        Ok(())
    }
}
