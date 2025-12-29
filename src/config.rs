use serde::Deserialize;
use std::path::Path;
use anyhow::Result;

#[derive(Debug, Deserialize, Clone)]
pub struct FuseRuleConfig {
    pub engine: EngineConfig,
    pub schema: Vec<FieldDef>,
    pub rules: Vec<RuleConfig>,
    pub agents: Vec<AgentConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FieldDef {
    pub name: String,
    pub data_type: String, // "int32", "float64", "utf8", "bool"
}

#[derive(Debug, Deserialize, Clone)]
pub struct EngineConfig {
    pub persistence_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RuleConfig {
    pub id: String,
    pub name: String,
    pub predicate: String,
    pub action: String,
    pub window_seconds: Option<u64>,
    #[serde(default = "default_version")]
    pub version: u32,
}

fn default_version() -> u32 { 1 }

#[derive(Debug, Deserialize, Clone)]
pub struct AgentConfig {
    pub name: String,
    pub r#type: String, // "logger", "webhook", etc.
    pub url: Option<String>,
}

impl FuseRuleConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .build()?;
        
        let config: FuseRuleConfig = settings.try_deserialize()?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let mut agent_names = std::collections::HashSet::new();
        for agent in &self.agents {
            if !agent_names.insert(&agent.name) {
                anyhow::bail!("Duplicate agent name: {}", agent.name);
            }
            if agent.r#type == "webhook" && agent.url.is_none() {
                anyhow::bail!("Webhook agent '{}' missing URL", agent.name);
            }
        }

        let mut rule_ids = std::collections::HashSet::new();
        for rule in &self.rules {
            if !rule_ids.insert(&rule.id) {
                anyhow::bail!("Duplicate rule ID: {}", rule.id);
            }
            if !agent_names.contains(&rule.action) {
                anyhow::bail!("Rule '{}' references unknown agent '{}'", rule.id, rule.action);
            }
            if rule.predicate.trim().is_empty() {
                anyhow::bail!("Rule '{}' has an empty predicate", rule.id);
            }
        }

        if self.schema.is_empty() {
            anyhow::bail!("Configuration must define a schema");
        }

        Ok(())
    }
}
