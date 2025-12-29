use clap::{Parser, Subcommand};
use arrow_rule_agent::RuleEngine;
use arrow_rule_agent::config::FuseRuleConfig;
use arrow_rule_agent::server::FuseRuleServer;
use std::sync::Arc;
use tokio::sync::RwLock;
use anyhow::Result;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    // 0. Initialize Tracing
    tracing_subscriber::fmt::init();
    
    let args = Cli::parse();

    match &args.command {
        Commands::Run { config, port } => {
            println!("🔥 Initializing FuseRule Daemon...");
            
            // 1. Load Config
            let config_data = FuseRuleConfig::from_file(config)?;
            
            // 2. Build Engine
            let engine = RuleEngine::from_config(config_data.clone()).await?;
            
            // 3. (Optional) In a real product, we'd add the rules from the config here too
            // For this version, let's assume the user wants the server to start.
            
            
            // 4. Start Server
            let server = FuseRuleServer::new(Arc::new(RwLock::new(engine)), config.to_string());
            server.run(*port).await?;
        }
    }

    Ok(())
}
