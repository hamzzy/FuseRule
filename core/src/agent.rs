use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tracing::{debug, info, warn};

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


// Implementations moved to fuse-rule-agents crate

