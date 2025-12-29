use crate::config::FuseRuleConfig;
use crate::evaluator::{CompiledRuleEdge, DataFusionEvaluator};
use crate::rule::Rule;
use crate::state::PredicateResult;
use arrow::array::{Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use anyhow::{Context, Result};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::sync::Arc;

pub struct RuleDebugger {
    evaluator: DataFusionEvaluator,
    schema: Arc<Schema>,
    last_batch: Option<RecordBatch>,
    breakpoints: std::collections::HashSet<String>,
}

impl RuleDebugger {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            evaluator: DataFusionEvaluator::new(),
            schema,
            last_batch: None,
            breakpoints: std::collections::HashSet::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut rl = DefaultEditor::new()?;
        let _ = rl.load_history(".fuserule_debug_history");

        println!("🐛 FuseRule Rule Debugger");
        println!("Commands:");
        println!("  load <config>  - Load rules from config file");
        println!("  batch <json>   - Load test batch from JSON");
        println!("  step <rule_id> - Step through rule evaluation");
        println!("  break <id>     - Set breakpoint on rule");
        println!("  unbreak <id>   - Remove breakpoint");
        println!("  list           - List all rules");
        println!("  breakpoints    - Show breakpoints");
        println!("  run            - Run evaluation on current batch");
        println!("  help           - Show this help");
        println!("  quit/exit      - Exit debugger");
        println!();

        loop {
            let readline = rl.readline("debugger> ");
            match readline {
                Ok(line) => {
                    let _ = rl.add_history_entry(line.as_str());
                    let trimmed = line.trim();
                    
                    if trimmed.is_empty() {
                        continue;
                    }

                    let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();
                    let command = parts[0];
                    let args = parts.get(1).unwrap_or(&"");

                    match command {
                        "load" => {
                            if let Err(e) = self.handle_load(args).await {
                                eprintln!("❌ Error: {}", e);
                            }
                        }
                        "batch" | "b" => {
                            if let Err(e) = self.handle_batch(args).await {
                                eprintln!("❌ Error: {}", e);
                            }
                        }
                        "step" | "s" => {
                            if let Err(e) = self.handle_step(args).await {
                                eprintln!("❌ Error: {}", e);
                            }
                        }
                        "break" => {
                            self.handle_break(args);
                        }
                        "unbreak" => {
                            self.handle_unbreak(args);
                        }
                        "list" | "l" => {
                            self.handle_list();
                        }
                        "breakpoints" | "bp" => {
                            self.handle_breakpoints();
                        }
                        "run" | "r" => {
                            if let Err(e) = self.handle_run().await {
                                eprintln!("❌ Error: {}", e);
                            }
                        }
                        "help" | "h" | "?" => {
                            self.show_help();
                        }
                        "quit" | "exit" | "q" => {
                            println!("👋 Goodbye!");
                            break;
                        }
                        _ => {
                            eprintln!("❌ Unknown command: {}. Type 'help' for commands.", command);
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break;
                }
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    break;
                }
            }
        }

        let _ = rl.save_history(".fuserule_debug_history");
        Ok(())
    }

    async fn handle_load(&mut self, config_path: &str) -> Result<()> {
        if config_path.is_empty() {
            eprintln!("Usage: load <config_file>");
            return Ok(());
        }

        let config = FuseRuleConfig::from_file(config_path)?;
        
        // Update schema from config
        let mut fields = Vec::new();
        for f in config.schema {
            let dt = match f.data_type.as_str() {
                "int32" => DataType::Int32,
                "int64" => DataType::Int64,
                "float32" => DataType::Float32,
                "float64" => DataType::Float64,
                "bool" => DataType::Boolean,
                "utf8" | "string" => DataType::Utf8,
                _ => DataType::Utf8,
            };
            fields.push(Field::new(f.name, dt, true));
        }
        self.schema = Arc::new(Schema::new(fields));
        
        println!("✅ Loaded config from: {}", config_path);
        println!("   Rules: {}", config.rules.len());
        println!("   Schema fields: {}", self.schema.fields().len());
        for field in self.schema.fields() {
            println!("     - {} ({:?})", field.name(), field.data_type());
        }

        Ok(())
    }

    async fn handle_batch(&mut self, json_str: &str) -> Result<()> {
        if json_str.is_empty() {
            eprintln!("Usage: batch <json_data>");
            eprintln!("   Example: batch '[{\"price\": 150, \"volume\": 1000}]'");
            return Ok(());
        }

        // Use arrow-json to parse JSON into RecordBatch
        use arrow_json::ReaderBuilder;
        use std::io::Cursor;

        let json_value: serde_json::Value = serde_json::from_str(json_str)
            .context("Failed to parse JSON")?;

        let json_data = serde_json::to_vec(&json_value)?;
        let cursor = Cursor::new(json_data);

        let reader = ReaderBuilder::new(self.schema.clone())
            .build(cursor)
            .context("Failed to create JSON reader")?;

        // Read first batch
        for batch_result in reader {
            match batch_result {
                Ok(batch) => {
                    self.last_batch = Some(batch.clone());
                    println!("✅ Loaded batch: {} rows", batch.num_rows());
                    println!("   Columns:");
                    for field in batch.schema().fields() {
                        println!("     - {} ({:?})", field.name(), field.data_type());
                    }
                    return Ok(());
                }
                Err(e) => {
                    anyhow::bail!("Failed to read batch: {}", e);
                }
            }
        }

        anyhow::bail!("No data found in JSON");
    }

    async fn handle_step(&mut self, rule_id: &str) -> Result<()> {
        if rule_id.is_empty() {
            eprintln!("Usage: step <rule_id> [predicate]");
            eprintln!("   Example: step r1 'price > 100'");
            return Ok(());
        }

        let batch = match &self.last_batch {
            Some(b) => b,
            None => {
                eprintln!("❌ No batch loaded. Use 'batch <json>' first.");
                return Ok(());
            }
        };

        // Parse predicate if provided, otherwise use default
        let parts: Vec<&str> = rule_id.splitn(2, ' ').collect();
        let rule_id = parts[0];
        let predicate = parts.get(1)
            .map(|s| s.trim_matches('\'').trim_matches('"').to_string())
            .unwrap_or_else(|| "price > 100".to_string());

        // Create a test rule for stepping
        let rule = Rule {
            id: rule_id.to_string(),
            name: format!("Debug Rule {}", rule_id),
            predicate,
            action: "logger".to_string(),
            window_seconds: None,
            version: 1,
            enabled: true,
        };

        println!("🔍 Stepping through rule: {}", rule_id);
        println!("   Predicate: {}", rule.predicate);
        println!("   Batch: {} rows", batch.num_rows());

        // Compile
        println!("📝 Compiling predicate...");
        let compiled = self.evaluator.compile(rule.clone(), &self.schema)?;
        println!("   ✅ Compiled successfully");
        if compiled.has_aggregates {
            println!("   ⚠️  Contains aggregate functions");
        }

        // Check breakpoint
        if self.breakpoints.contains(rule_id) {
            println!("⏸️  Breakpoint hit at rule: {}", rule_id);
            println!("   Press Enter to continue...");
            let mut line = String::new();
            std::io::stdin().read_line(&mut line)?;
        }

        // Evaluate
        println!("⚙️  Evaluating predicate...");
        let results = self.evaluator
            .evaluate_batch(batch, &[compiled], &[vec![]])
            .await?;

        let result = results[0].0;
        println!("📊 Result: {:?}", result);
        println!("   {}", if matches!(result, PredicateResult::True) {
            "✅ Predicate is TRUE"
        } else {
            "❌ Predicate is FALSE"
        });

        if let Some(matched_batch) = &results[0].1 {
            println!("   Matched rows: {}", matched_batch.num_rows());
        }

        Ok(())
    }

    fn handle_break(&mut self, rule_id: &str) {
        if rule_id.is_empty() {
            eprintln!("Usage: break <rule_id>");
            return;
        }
        self.breakpoints.insert(rule_id.to_string());
        println!("✅ Breakpoint set on rule: {}", rule_id);
    }

    fn handle_unbreak(&mut self, rule_id: &str) {
        if rule_id.is_empty() {
            eprintln!("Usage: unbreak <rule_id>");
            return;
        }
        if self.breakpoints.remove(rule_id) {
            println!("✅ Breakpoint removed from rule: {}", rule_id);
        } else {
            println!("⚠️  No breakpoint found on rule: {}", rule_id);
        }
    }

    fn handle_list(&self) {
        println!("📋 Loaded rules:");
        println!("   (Use 'load <config>' to load rules from config)");
    }

    fn handle_breakpoints(&self) {
        if self.breakpoints.is_empty() {
            println!("No breakpoints set");
        } else {
            println!("⏸️  Breakpoints:");
            for bp in &self.breakpoints {
                println!("   - {}", bp);
            }
        }
    }

    async fn handle_run(&mut self) -> Result<()> {
        let batch = match &self.last_batch {
            Some(b) => b,
            None => {
                eprintln!("❌ No batch loaded. Use 'batch <json>' first.");
                return Ok(());
            }
        };

        println!("🚀 Running evaluation on batch ({} rows)...", batch.num_rows());
        println!("   (This is a simplified run - use 'step' for detailed debugging)");

        Ok(())
    }

    fn show_help(&self) {
        println!("Commands:");
        println!("  load <config>  - Load rules from config file");
        println!("  batch <json>   - Load test batch from JSON");
        println!("  step <rule_id> - Step through rule evaluation");
        println!("  break <id>     - Set breakpoint on rule");
        println!("  unbreak <id>   - Remove breakpoint");
        println!("  list           - List all rules");
        println!("  breakpoints    - Show breakpoints");
        println!("  run            - Run evaluation on current batch");
        println!("  help           - Show this help");
        println!("  quit/exit      - Exit debugger");
    }
}

