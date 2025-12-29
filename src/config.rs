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
}

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
        Ok(config)
    }
}
