use async_trait::async_trait;
use tracing::{debug, info, warn};
use fuse_rule_core::agent::{Agent, Activation};
use arrow::array::Array;

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
            let field_names: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
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
    template: Option<handlebars::Handlebars<'static>>,
}

impl WebhookAgent {
    pub fn new(url: String, template: Option<String>) -> Self {
        // Use connection pooling with proper configuration
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(10)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        // Compile Handlebars template if provided
        let compiled_template = template.and_then(|t| {
            let mut handlebars = handlebars::Handlebars::new();
            handlebars.set_strict_mode(true);
            if handlebars.register_template_string("webhook", &t).is_ok() {
                Some(handlebars)
            } else {
                None
            }
        });

        Self {
            url,
            client,
            template: compiled_template,
        }
    }
}

#[async_trait]
impl Agent for WebhookAgent {
    fn name(&self) -> &str {
        "webhook"
    }

    async fn execute(&self, activation: &Activation) -> anyhow::Result<()> {
        // Build template context
        let mut context = serde_json::json!({
            "rule_id": activation.rule_id,
            "rule_name": activation.rule_name,
            "action": activation.action,
            "count": activation.context.as_ref().map(|b| b.num_rows()).unwrap_or(0),
            "matched_rows": activation.context.as_ref().map(|b| b.num_rows()).unwrap_or(0),
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        // Add matched rows data if available (rich context)
        let mut matched_data = Vec::new();
        if let Some(batch) = &activation.context {
            // Convert RecordBatch to JSON for rich context
            let schema = batch.schema();
            for row_idx in 0..batch.num_rows() {
                let mut row = serde_json::Map::new();
                for col_idx in 0..batch.num_columns() {
                    let field = schema.field(col_idx);
                    let array = batch.column(col_idx);
                    // Convert array value to JSON (simplified - handles common types)
                    let value = match array.data_type() {
                        arrow::datatypes::DataType::Int32 => {
                            if let Some(arr) =
                                array.as_any().downcast_ref::<arrow::array::Int32Array>()
                            {
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
                            if let Some(arr) =
                                array.as_any().downcast_ref::<arrow::array::Int64Array>()
                            {
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
                            if let Some(arr) =
                                array.as_any().downcast_ref::<arrow::array::Float64Array>()
                            {
                                if !arr.is_null(row_idx) {
                                    serde_json::Value::Number(
                                        serde_json::Number::from_f64(arr.value(row_idx))
                                            .unwrap_or_else(|| serde_json::Number::from(0)),
                                    )
                                } else {
                                    serde_json::Value::Null
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        }
                        arrow::datatypes::DataType::Boolean => {
                            if let Some(arr) =
                                array.as_any().downcast_ref::<arrow::array::BooleanArray>()
                            {
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
                            if let Some(arr) =
                                array.as_any().downcast_ref::<arrow::array::StringArray>()
                            {
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
                matched_data.push(serde_json::Value::Object(row));
            }
            context.as_object_mut().unwrap().insert(
                "matched_data".to_string(),
                serde_json::Value::Array(matched_data.clone()),
            );
            context.as_object_mut().unwrap().insert(
                "matched_rows".to_string(),
                serde_json::Value::Array(matched_data),
            );
        }

        // Render payload using template or default JSON
        let payload: serde_json::Value = if let Some(ref template) = self.template {
            // Use Handlebars template
            match template.render("webhook", &context) {
                Ok(rendered) => {
                    // Try to parse as JSON, fallback to string
                    serde_json::from_str(&rendered)
                        .unwrap_or_else(|_| serde_json::json!({ "text": rendered }))
                }
                Err(e) => {
                    // Template rendering failed, fallback to default JSON
                    warn!(error = %e, "Template rendering failed, using default payload");
                    context
                }
            }
        } else {
            // Use default JSON format
            context
        };

        debug!(url = %self.url, rule_id = %activation.rule_id, "Sending webhook");

        // In a real production system, we might want to handle retries here
        self.client.post(&self.url).json(&payload).send().await?;

        Ok(())
    }
}
