use crate::config::FuseRuleConfig;
use crate::evaluator::DataFusionEvaluator;
use crate::rule::Rule;
use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
pub struct ValidateArgs {
    /// Path to configuration file
    #[arg(short, long)]
    config: String,
    
    /// Rule predicate to validate
    #[arg(short, long)]
    predicate: Option<String>,
}

pub async fn validate_rule(config_path: &str, predicate: Option<&str>) -> Result<()> {
    let config = FuseRuleConfig::from_file(config_path)?;
    
    if let Some(pred) = predicate {
        // Validate specific predicate
        let evaluator = DataFusionEvaluator::new();
        let schema = crate::lib::RuleEngine::from_config(config.clone())
            .map(|e| e.schema())?;
        
        let test_rule = Rule {
            id: "test".to_string(),
            name: "Test Rule".to_string(),
            predicate: pred.to_string(),
            action: "logger".to_string(),
            window_seconds: None,
            version: 1,
            enabled: true,
        };
        
        match evaluator.compile(test_rule, &schema) {
            Ok(_) => {
                println!("✅ Predicate is valid");
                Ok(())
            }
            Err(e) => {
                eprintln!("❌ Predicate validation failed: {}", e);
                Err(e.into())
            }
        }
    } else {
        // Validate all rules in config
        let evaluator = DataFusionEvaluator::new();
        let engine = crate::lib::RuleEngine::from_config(config.clone()).await?;
        let schema = engine.schema();
        
        let mut all_valid = true;
        for rule_cfg in &config.rules {
            let rule = Rule {
                id: rule_cfg.id.clone(),
                name: rule_cfg.name.clone(),
                predicate: rule_cfg.predicate.clone(),
                action: rule_cfg.action.clone(),
                window_seconds: rule_cfg.window_seconds,
                version: rule_cfg.version,
                enabled: rule_cfg.enabled,
            };
            
            match evaluator.compile(rule.clone(), &schema) {
                Ok(compiled) => {
                    println!("✅ Rule '{}' ({}): Valid", rule.name, rule.id);
                    if compiled.has_aggregates {
                        println!("   └─ Contains aggregate functions");
                    }
                }
                Err(e) => {
                    eprintln!("❌ Rule '{}' ({}): Invalid - {}", rule.name, rule.id, e);
                    all_valid = false;
                }
            }
        }
        
        if all_valid {
            println!("\n✅ All rules are valid");
            Ok(())
        } else {
            anyhow::bail!("Some rules failed validation");
        }
    }
}

