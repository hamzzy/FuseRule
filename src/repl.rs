use crate::config::FuseRuleConfig;
use crate::RuleEngine;
use arrow::array::{Float64Array, Int32Array, StringArray, BooleanArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_json::ReaderBuilder;
use anyhow::{Context, Result};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde_json::Value;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Repl {
    engine: Arc<RwLock<RuleEngine>>,
    schema: Arc<Schema>,
}

impl Repl {
    pub fn new(engine: Arc<RwLock<RuleEngine>>, schema: Arc<Schema>) -> Self {
        Self { engine, schema }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut rl = DefaultEditor::new()?;
        let _ = rl.load_history(".fuserule_history");

        println!("🔥 FuseRule Interactive REPL");
        println!("Commands:");
        println!("  ingest <json>  - Ingest JSON data");
        println!("  state          - Show all rule states");
        println!("  state <id>     - Show state for specific rule");
        println!("  rules          - List all rules");
        println!("  eval <pred>    - Evaluate predicate on last batch");
        println!("  help           - Show this help");
        println!("  quit/exit      - Exit REPL");
        println!();

        loop {
            let readline = rl.readline("fuserule> ");
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
                        "ingest" | "i" => {
                            if let Err(e) = self.handle_ingest(args).await {
                                eprintln!("❌ Error: {}", e);
                            }
                        }
                        "state" | "s" => {
                            if let Err(e) = self.handle_state(args).await {
                                eprintln!("❌ Error: {}", e);
                            }
                        }
                        "rules" | "r" => {
                            if let Err(e) = self.handle_rules().await {
                                eprintln!("❌ Error: {}", e);
                            }
                        }
                        "eval" | "e" => {
                            if let Err(e) = self.handle_eval(args).await {
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

        let _ = rl.save_history(".fuserule_history");
        Ok(())
    }

    async fn handle_ingest(&self, json_str: &str) -> Result<()> {
        if json_str.is_empty() {
            eprintln!("Usage: ingest <json_data>");
            return Ok(());
        }

        // Parse JSON
        let json_value: Value = serde_json::from_str(json_str)
            .context("Failed to parse JSON")?;

        // Convert to RecordBatch
        let json_data = serde_json::to_vec(&json_value)?;
        let cursor = Cursor::new(json_data);

        let reader = ReaderBuilder::new(self.schema.clone())
            .build(cursor)
            .context("Failed to create JSON reader")?;

        let mut engine = self.engine.write().await;
        let mut batch_count = 0;

        for batch_result in reader {
            match batch_result {
                Ok(batch) => {
                    batch_count += 1;
                    let traces = engine.process_batch(&batch).await?;
                    
                    println!("✅ Ingested batch #{} ({} rows)", batch_count, batch.num_rows());
                    
                    for trace in traces {
                        if trace.action_fired {
                            println!(
                                "  🔔 Rule '{}' ({}) {} -> {}",
                                trace.rule_name,
                                trace.rule_id,
                                match trace.transition.as_str() {
                                    "Activated" => "⚡ ACTIVATED",
                                    "Deactivated" => "🔻 DEACTIVATED",
                                    _ => "",
                                },
                                if matches!(trace.result, crate::state::PredicateResult::True) {
                                    "TRUE"
                                } else {
                                    "FALSE"
                                }
                            );
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ Failed to read batch: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn handle_state(&self, rule_id: &str) -> Result<()> {
        let engine = self.engine.read().await;

        if rule_id.is_empty() {
            // Show all states
            println!("📊 Rule States:");
            for rule in &engine.rules {
                let last_result = engine.state.get_last_result(&rule.rule.id).await?;
                let window_size = engine.window_buffers
                    .get(&rule.rule.id)
                    .map(|b| {
                        b.get_batches().iter().map(|batch| batch.num_rows()).sum::<usize>()
                    })
                    .unwrap_or(0);

                println!(
                    "  {} {} [{}] - Window: {} rows",
                    if matches!(last_result, crate::state::PredicateResult::True) {
                        "🟢"
                    } else {
                        "⚪"
                    },
                    rule.rule.name,
                    rule.rule.id,
                    window_size
                );
            }
        } else {
            // Show specific rule state
            let rule = engine.rules.iter().find(|r| r.rule.id == rule_id);
            if let Some(rule) = rule {
                let last_result = engine.state.get_last_result(&rule_id).await?;
                let last_transition = engine.state.get_last_transition_time(&rule_id).await?;
                let window_size = engine.window_buffers
                    .get(&rule_id)
                    .map(|b| {
                        b.get_batches().iter().map(|batch| batch.num_rows()).sum::<usize>()
                    })
                    .unwrap_or(0);

                println!("📊 Rule State: {}", rule.rule.name);
                println!("  ID: {}", rule.rule.id);
                println!("  Predicate: {}", rule.rule.predicate);
                println!("  Current State: {:?}", last_result);
                println!("  Window Size: {} rows", window_size);
                println!("  Enabled: {}", rule.rule.enabled);
                if let Some(ts) = last_transition {
                    println!("  Last Transition: {}", ts);
                }
            } else {
                eprintln!("❌ Rule not found: {}", rule_id);
            }
        }

        Ok(())
    }

    async fn handle_rules(&self) -> Result<()> {
        let engine = self.engine.read().await;

        println!("📋 Rules:");
        for (i, rule) in engine.rules.iter().enumerate() {
            println!(
                "  {}. {} ({}) - {} [{}]",
                i + 1,
                rule.rule.name,
                rule.rule.id,
                if rule.rule.enabled { "✅" } else { "❌" },
                rule.rule.predicate
            );
        }

        Ok(())
    }

    async fn handle_eval(&self, predicate: &str) -> Result<()> {
        if predicate.is_empty() {
            eprintln!("Usage: eval <predicate>");
            return Ok(());
        }

        eprintln!("⚠️  Eval command requires last batch - use 'ingest' first");
        eprintln!("   This feature will be enhanced in the debugger mode");
        Ok(())
    }

    fn show_help(&self) {
        println!("Commands:");
        println!("  ingest <json>  - Ingest JSON data");
        println!("  state          - Show all rule states");
        println!("  state <id>     - Show state for specific rule");
        println!("  rules          - List all rules");
        println!("  eval <pred>    - Evaluate predicate on last batch");
        println!("  help           - Show this help");
        println!("  quit/exit      - Exit REPL");
    }
}

