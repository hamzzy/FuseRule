use anyhow::Result;
use arrow_rule_agent::config::{FuseRuleConfig, SourceConfig};
use arrow_rule_agent::ingestion::{KafkaIngestion, SharedEngine, WebSocketIngestion};
use arrow_rule_agent::server::FuseRuleServer;
use arrow_rule_agent::RuleEngine;
use clap::{Parser, Subcommand};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Parser)]
#[command(name = "fuserule")]
#[command(about = "High-performance Arrow-based Rule Engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the FuseRule server
    Run {
        /// Path to the configuration file
        #[arg(short, long, default_value = "fuse_rule_config.yaml")]
        config: String,

        /// Port to listen on
        #[arg(short, long, default_value_t = 3030)]
        port: u16,
    },
    /// Validate rules in configuration file
    Validate {
        /// Path to the configuration file
        #[arg(short, long, default_value = "fuse_rule_config.yaml")]
        config: String,
        
        /// Specific predicate to validate (optional)
        #[arg(short, long)]
        predicate: Option<String>,
    },
    /// Start interactive REPL mode
    Repl {
        /// Path to the configuration file
        #[arg(short, long, default_value = "fuse_rule_config.yaml")]
        config: String,
    },
    /// Start rule debugger
    Debug {
        /// Path to the configuration file
        #[arg(short, long, default_value = "fuse_rule_config.yaml")]
        config: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // 0. Initialize Tracing
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    match &args.command {
        Commands::Validate { config, predicate } => {
            arrow_rule_agent::cli::validate_rule(config, predicate.as_deref()).await?;
        }
        Commands::Repl { config } => {
            println!("🔥 Starting FuseRule REPL...");
            let config_data = FuseRuleConfig::from_file(config)?;
            let engine = RuleEngine::from_config(config_data.clone()).await?;
            let shared_engine = Arc::new(RwLock::new(engine));
            let schema = shared_engine.read().await.schema();
            let mut repl = arrow_rule_agent::repl::Repl::new(shared_engine, schema);
            repl.run().await?;
        }
        Commands::Debug { config } => {
            println!("🐛 Starting FuseRule Debugger...");
            let config_data = FuseRuleConfig::from_file(config)?;
            let schema = RuleEngine::from_config(config_data.clone())
                .await
                .map(|e| e.schema())?;
            let mut debugger = arrow_rule_agent::debugger::RuleDebugger::new(schema);
            debugger.run().await?;
        }
        Commands::Run { config, port } => {
            println!("🔥 Initializing FuseRule Daemon...");

            // 1. Load Config
            let config_data = FuseRuleConfig::from_file(config)?;

            // 2. Build Engine
            let engine = RuleEngine::from_config(config_data.clone()).await?;

            // 3. (Optional) In a real product, we'd add the rules from the config here too
            // For this version, let's assume the user wants the server to start.

            // 4. Start Server with rate limiting and API key auth
            let rate_limit = config_data.engine.ingest_rate_limit;
            let api_keys = config_data.engine.api_keys.clone();
            let shared_engine = Arc::new(RwLock::new(engine));
            
            let server = FuseRuleServer::new(
                shared_engine.clone(),
                config.to_string(),
                rate_limit,
                api_keys,
            );
            
            // 5. Start data ingestion sources (Kafka, WebSocket)
            let mut source_handles = Vec::new();
            for source in &config_data.sources {
                match source {
                    SourceConfig::Kafka {
                        brokers,
                        topic,
                        group_id,
                        auto_commit,
                    } => {
                        info!("Starting Kafka ingestion: topic={}, group_id={}", topic, group_id);
                        let kafka = KafkaIngestion::new(
                            shared_engine.clone(),
                            brokers.clone(),
                            topic.clone(),
                            group_id.clone(),
                            *auto_commit,
                        );
                        let handle = tokio::spawn(async move {
                            if let Err(e) = kafka.run().await {
                                tracing::error!(error = %e, "Kafka ingestion error");
                            }
                        });
                        source_handles.push(handle);
                    }
                    SourceConfig::WebSocket { bind, max_connections } => {
                        info!("Starting WebSocket ingestion: bind={}, max_connections={}", bind, max_connections);
                        let ws = WebSocketIngestion::new(
                            shared_engine.clone(),
                            bind.clone(),
                            *max_connections,
                        );
                        let handle = tokio::spawn(async move {
                            if let Err(e) = ws.run().await {
                                tracing::error!(error = %e, "WebSocket ingestion error");
                            }
                        });
                        source_handles.push(handle);
                    }
                }
            }
            
            // Start HTTP server and wait for it
            let server_handle = tokio::spawn(async move {
                if let Err(e) = server.run(*port).await {
                    tracing::error!(error = %e, "HTTP server error");
                }
            });
            
            // Wait for all tasks
            tokio::select! {
                _ = server_handle => {},
                _ = async {
                    for handle in source_handles {
                        let _ = handle.await;
                    }
                } => {},
            }
        }
    }

    Ok(())
}
