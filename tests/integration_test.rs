use arrow_rule_agent::config::FuseRuleConfig;
use arrow_rule_agent::RuleEngine;
use arrow::array::{Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::test]
async fn test_rule_evaluation() {
    // Create a simple config
    let config = FuseRuleConfig {
        engine: arrow_rule_agent::config::EngineConfig {
            persistence_path: "test_state".to_string(),
            max_pending_batches: 1000,
            agent_concurrency: 10,
            ingest_rate_limit: None,
            api_keys: vec![],
        },
        schema: vec![
            arrow_rule_agent::config::FieldDef {
                name: "price".to_string(),
                data_type: "float64".to_string(),
            },
            arrow_rule_agent::config::FieldDef {
                name: "symbol".to_string(),
                data_type: "utf8".to_string(),
            },
        ],
        rules: vec![arrow_rule_agent::config::RuleConfig {
            id: "test_rule".to_string(),
            name: "High Price".to_string(),
            predicate: "price > 100".to_string(),
            action: "logger".to_string(),
            window_seconds: None,
            version: 1,
            enabled: true,
            state_ttl_seconds: None,
        }],
        agents: vec![arrow_rule_agent::config::AgentConfig {
            name: "logger".to_string(),
            r#type: "logger".to_string(),
            url: None,
            template: None,
        }],
        sources: vec![],
    };

    // Build engine
    let mut engine = RuleEngine::from_config(config).await.unwrap();

    // Create test batch
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
        Field::new("symbol", DataType::Utf8, true),
    ]);

    let price_array = Arc::new(Float64Array::from(vec![150.0, 50.0, 200.0]));
    let symbol_array = Arc::new(StringArray::from(vec!["AAPL", "GOOGL", "MSFT"]));

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![price_array, symbol_array],
    ).unwrap();

    // Process batch
    let traces = engine.process_batch(&batch).await.unwrap();

    // Verify rule activated for high prices
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].rule_id, "test_rule");
    assert!(matches!(traces[0].result, arrow_rule_agent::state::PredicateResult::True));
}

#[tokio::test]
async fn test_window_aggregation() {
    let config = FuseRuleConfig {
        engine: arrow_rule_agent::config::EngineConfig {
            persistence_path: "test_state2".to_string(),
            max_pending_batches: 1000,
            agent_concurrency: 10,
            ingest_rate_limit: None,
            api_keys: vec![],
        },
        schema: vec![
            arrow_rule_agent::config::FieldDef {
                name: "price".to_string(),
                data_type: "float64".to_string(),
            },
        ],
        rules: vec![arrow_rule_agent::config::RuleConfig {
            id: "window_rule".to_string(),
            name: "Window Test".to_string(),
            predicate: "price > 50".to_string(),
            action: "logger".to_string(),
            window_seconds: Some(10),
            version: 1,
            enabled: true,
            state_ttl_seconds: None,
        }],
        agents: vec![arrow_rule_agent::config::AgentConfig {
            name: "logger".to_string(),
            r#type: "logger".to_string(),
            url: None,
            template: None,
        }],
        sources: vec![],
    };

    let mut engine = RuleEngine::from_config(config).await.unwrap();

    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, true),
    ]);

    // First batch
    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Float64Array::from(vec![60.0]))],
    ).unwrap();

    let _traces1 = engine.process_batch(&batch1).await.unwrap();

    // Second batch (should use window)
    let batch2 = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Float64Array::from(vec![70.0]))],
    ).unwrap();

    let traces2 = engine.process_batch(&batch2).await.unwrap();
    assert_eq!(traces2.len(), 1);
}

