use crate::config::FuseRuleConfig;
use crate::evaluator::{DataFusionEvaluator, RuleEvaluator};
use crate::rule::Rule;
use crate::RuleEngine;
use anyhow::Result;
use clap::Parser;
use std::collections::HashSet;

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
        let schema = RuleEngine::from_config(config.clone())
            .await
            .map(|e| e.schema())?;

        let test_rule = Rule {
            id: "test".to_string(),
            name: "Test Rule".to_string(),
            predicate: pred.to_string(),
            action: "logger".to_string(),
            window_seconds: None,
            version: 1,
            enabled: true,
            description: None,
            tags: Vec::new(),
        };

        match evaluator.compile(test_rule, &schema) {
            Ok(_) => {
                println!("✅ Predicate is valid");
                Ok(())
            }
            Err(e) => {
                eprintln!("❌ Predicate validation failed: {}", e);
                Err(e)
            }
        }
    } else {
        // Validate all rules in config
        let evaluator = DataFusionEvaluator::new();
        let engine = RuleEngine::from_config(config.clone()).await?;
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
                description: rule_cfg.description.clone(),
                tags: rule_cfg.tags.clone(),
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

/// Lint rules in a configuration file
pub async fn lint_rules(config_path: &str) -> Result<()> {
    let config = FuseRuleConfig::from_file(config_path)?;
    let evaluator = DataFusionEvaluator::new();
    let engine = RuleEngine::from_config(config.clone()).await?;
    let schema = engine.schema();

    println!("🔍 Linting rules in: {}\n", config_path);
    
    let mut issues = Vec::new();
    let mut rule_ids = HashSet::new();
    let mut rule_names = HashSet::new();

    for rule_cfg in &config.rules {
        // Check for duplicate IDs
        if !rule_ids.insert(&rule_cfg.id) {
            issues.push(format!("❌ Duplicate rule ID: '{}'", rule_cfg.id));
        }

        // Check for duplicate names
        if !rule_names.insert(&rule_cfg.name) {
            issues.push(format!("⚠️  Duplicate rule name: '{}' (ID: {})", rule_cfg.name, rule_cfg.id));
        }

        // Check for empty predicate
        if rule_cfg.predicate.trim().is_empty() {
            issues.push(format!("❌ Rule '{}' ({}): Empty predicate", rule_cfg.name, rule_cfg.id));
        }

        // Check for missing description
        if rule_cfg.description.is_none() || rule_cfg.description.as_ref().unwrap().trim().is_empty() {
            issues.push(format!("⚠️  Rule '{}' ({}): Missing description", rule_cfg.name, rule_cfg.id));
        }

        // Check for missing tags
        if rule_cfg.tags.is_empty() {
            issues.push(format!("ℹ️  Rule '{}' ({}): No tags specified", rule_cfg.name, rule_cfg.id));
        }

        // Validate predicate syntax
        let rule = Rule {
            id: rule_cfg.id.clone(),
            name: rule_cfg.name.clone(),
            predicate: rule_cfg.predicate.clone(),
            action: rule_cfg.action.clone(),
            window_seconds: rule_cfg.window_seconds,
            version: rule_cfg.version,
            enabled: rule_cfg.enabled,
            description: rule_cfg.description.clone(),
            tags: rule_cfg.tags.clone(),
        };

        match evaluator.compile(rule, &schema) {
            Ok(compiled) => {
                if compiled.has_aggregates && rule_cfg.window_seconds.is_none() {
                    issues.push(format!(
                        "⚠️  Rule '{}' ({}): Uses aggregate functions but has no window_seconds",
                        rule_cfg.name, rule_cfg.id
                    ));
                }
            }
            Err(e) => {
                issues.push(format!(
                    "❌ Rule '{}' ({}): Invalid predicate - {}",
                    rule_cfg.name, rule_cfg.id, e
                ));
            }
        }
    }

    // Print results
    if issues.is_empty() {
        println!("✅ No linting issues found!");
        Ok(())
    } else {
        println!("Found {} issue(s):\n", issues.len());
        for issue in &issues {
            println!("  {}", issue);
        }
        println!();
        
        let errors = issues.iter().filter(|i| i.starts_with("❌")).count();
        if errors > 0 {
            anyhow::bail!("Found {} error(s) that must be fixed", errors);
        } else {
            println!("⚠️  Found {} warning(s) and {} info message(s)", 
                issues.iter().filter(|i| i.starts_with("⚠️")).count(),
                issues.iter().filter(|i| i.starts_with("ℹ️")).count());
            Ok(())
        }
    }
}

/// Test rules with validation mode
pub async fn test_rules(config_path: &str) -> Result<()> {
    let config = FuseRuleConfig::from_file(config_path)?;
    
    println!("🧪 Testing rules in: {}\n", config_path);
    
    // First validate all rules
    println!("1️⃣  Validating rule syntax...");
    validate_rule(config_path, None).await?;
    println!();

    // Then lint
    println!("2️⃣  Linting rules...");
    lint_rules(config_path).await?;
    println!();

    // Check rule metadata
    println!("3️⃣  Checking rule metadata...");
    let mut metadata_issues = 0;
    for rule_cfg in &config.rules {
        if rule_cfg.description.is_none() || rule_cfg.description.as_ref().unwrap().trim().is_empty() {
            println!("  ⚠️  Rule '{}' ({}): Missing description", rule_cfg.name, rule_cfg.id);
            metadata_issues += 1;
        }
        if rule_cfg.tags.is_empty() {
            println!("  ℹ️  Rule '{}' ({}): No tags", rule_cfg.name, rule_cfg.id);
        }
    }
    if metadata_issues == 0 {
        println!("  ✅ All rules have descriptions");
    }
    println!();

    println!("✅ All tests passed!");
    Ok(())
}
