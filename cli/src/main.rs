mod cli;
mod debugger;
mod repl;
mod server;

use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete;
use fuse_rule_core::config::{FuseRuleConfig, SourceConfig};
use fuse_rule_connectors::{KafkaIngestion, WebSocketIngestion};
use crate::server::FuseRuleServer;
use fuse_rule_core::RuleEngine;
use fuse_rule_agents::{LoggerAgent, WebhookAgent};
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
    /// Lint rules in configuration file
    Lint {
        /// Path to the configuration file
        #[arg(short, long, default_value = "fuse_rule_config.yaml")]
        config: String,
    },
    /// Test rules with validation mode
    Test {
        /// Path to the configuration file
        #[arg(short, long, default_value = "fuse_rule_config.yaml")]
        config: String,
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
    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

async fn build_engine(config: &FuseRuleConfig) -> Result<RuleEngine> {
    let mut engine = RuleEngine::from_config(config.clone()).await?;

    for agent_cfg in &config.agents {
        match agent_cfg.r#type.as_str() {
             "logger" => {
                 engine.add_agent(agent_cfg.name.clone(), Arc::new(LoggerAgent));
             }
             "webhook" => {
                 if let Some(url) = &agent_cfg.url {
                     engine.add_agent(
                         agent_cfg.name.clone(),
                         Arc::new(WebhookAgent::new(
                                url.clone(),
                                agent_cfg.template.clone(),
                         )),
                     );
                 }
             }
             _ => println!("Warning: Unknown agent type '{}'", agent_cfg.r#type),
        }
    }
    Ok(engine)
}

#[tokio::main]
async fn main() -> Result<()> {
    // 0. Initialize Tracing
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    match args.command {
        Commands::Validate { config, predicate } => {
            crate::cli::validate_rule(&config, predicate.as_deref()).await?;
        }
        Commands::Lint { config } => {
            crate::cli::lint_rules(&config).await?;
        }
        Commands::Test { config } => {
            crate::cli::test_rules(&config).await?;
        }
        Commands::Repl { config } => {
            println!("🔥 Starting FuseRule REPL...");
            let config_data = FuseRuleConfig::from_file(&config)?;
            let engine = build_engine(&config_data).await?;
            let shared_engine = Arc::new(RwLock::new(engine));
            let schema = shared_engine.read().await.schema();
            let mut repl = crate::repl::Repl::new(shared_engine, schema);
            repl.run().await?;
        }
        Commands::Debug { config } => {
            println!("🐛 Starting FuseRule Debugger...");
            let config_data = FuseRuleConfig::from_file(&config)?;
            let engine = build_engine(&config_data).await?;
            let schema = engine.schema();
            let mut debugger = crate::debugger::RuleDebugger::new(schema);
            debugger.run().await?;
        }
        Commands::Completions { shell } => {
            clap_complete::generate(
                shell,
                &mut Cli::command(),
                "fuserule",
                &mut std::io::stdout(),
            );
        }
        Commands::Run { config, port } => {
            println!("🔥 Initializing FuseRule Daemon...");

            // 1. Load Config
            let config_data = FuseRuleConfig::from_file(&config)?;

            // 2. Build Engine
            let engine = build_engine(&config_data).await?;

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
                        info!(
                            "Starting Kafka ingestion: topic={}, group_id={}",
                            topic, group_id
                        );
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
                    SourceConfig::WebSocket {
                        bind,
                        max_connections,
                    } => {
                        info!(
                            "Starting WebSocket ingestion: bind={}, max_connections={}",
                            bind, max_connections
                        );
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
                if let Err(e) = server.run(port).await {
                    tracing::error!(error = %e, "HTTP server error");
                }
            });

            // Wait for all tasks
            // If there are no source handles, just wait for the server
            if source_handles.is_empty() {
                info!("No ingestion sources configured, waiting for server...");
                server_handle.await?;
            } else {
                tokio::select! {
                    _ = server_handle => {
                        info!("Server task completed");
                    },
                    _ = async {
                        for handle in source_handles {
                            let _ = handle.await;
                        }
                    } => {
                        info!("All ingestion sources completed");
                    },
                }
            }
        }
    }

    Ok(())
}
