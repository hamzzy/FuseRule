use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone)]
pub struct Activation {
    pub rule_id: String,
    pub rule_name: String,
    pub action: String,
    pub context: Option<RecordBatch>,
}

pub trait Agent: Send + Sync {
    fn name(&self) -> &str;
    fn execute(&self, activation: &Activation);
}

pub struct LoggerAgent;

impl Agent for LoggerAgent {
    fn name(&self) -> &str {
        "logger"
    }

    fn execute(&self, activation: &Activation) {
        let rows = activation.context.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        println!("[Agent: {}] Firing action '{}' for rule '{}' ({}) - Context: {} rows matched", 
            self.name(), activation.action, activation.rule_name, activation.rule_id, rows);
    }
}
