use anyhow::Result;
use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fuse_rule_core::udf::{BuiltinUdfs, UdfRegistry};
use fuse_rule_core::RuleEngine;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("🎯 FuseRule UDF Example\n");
    println!("This example demonstrates custom User-Defined Functions (UDFs)");
    println!("including is_weekend(), normalize_price(), and extract_domain()\n");

    // Create schema with various data types
    let schema = Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("decimals", DataType::Int32, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
    ]);

    // Create test data
    // Timestamps: Friday, Saturday, Sunday, Monday (Jan 5-8, 2024)
    let timestamps = Arc::new(Int64Array::from(vec![
        1704412800, // Friday
        1704499200, // Saturday
        1704585600, // Sunday
        1704672000, // Monday
    ]));

    let prices = Arc::new(Float64Array::from(vec![99.999, 100.123, 50.555, 75.678]));
    let decimals = Arc::new(Int32Array::from(vec![2, 2, 1, 2]));
    let emails = Arc::new(StringArray::from(vec![
        "user@example.com",
        "admin@test.org",
        "support@company.io",
        "sales@business.net",
    ]));
    let urls = Arc::new(StringArray::from(vec![
        "https://www.google.com/search",
        "http://localhost:8080/api",
        "https://github.com/user/repo",
        "https://docs.rs/datafusion",
    ]));

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![timestamps, prices, decimals, emails, urls],
    )?;

    println!("📊 Test Data:");
    println!("  Rows: {}", batch.num_rows());
    println!("  Columns: {}\n", batch.num_columns());

    // Example 1: Weekend Detection
    println!("=" .repeat(60));
    println!("Example 1: Weekend Detection with is_weekend()");
    println!("=" .repeat(60));

    let config = fuse_rule_core::config::FuseRuleConfig {
        engine: fuse_rule_core::config::EngineConfig {
            persistence_path: None,
            ingest_rate_limit: None,
            api_keys: vec![],
        },
        schema: vec![
            fuse_rule_core::config::SchemaField {
                name: "timestamp".to_string(),
                data_type: "int64".to_string(),
            },
            fuse_rule_core::config::SchemaField {
                name: "price".to_string(),
                data_type: "float64".to_string(),
            },
            fuse_rule_core::config::SchemaField {
                name: "decimals".to_string(),
                data_type: "int32".to_string(),
            },
            fuse_rule_core::config::SchemaField {
                name: "email".to_string(),
                data_type: "utf8".to_string(),
            },
            fuse_rule_core::config::SchemaField {
                name: "url".to_string(),
                data_type: "utf8".to_string(),
            },
        ],
        agents: vec![fuse_rule_core::config::AgentConfig {
            name: "logger".to_string(),
            agent_type: "logger".to_string(),
            url: None,
            template: None,
        }],
        rules: vec![
            fuse_rule_core::config::RuleConfig {
                id: "weekend_high_price".to_string(),
                name: "Weekend High Price Alert".to_string(),
                predicate: "is_weekend(timestamp) AND price > 100".to_string(),
                action: "logger".to_string(),
                window_seconds: None,
                version: 1,
                enabled: true,
                state_ttl_seconds: None,
            },
            fuse_rule_core::config::RuleConfig {
                id: "normalized_price_check".to_string(),
                name: "Normalized Price Check".to_string(),
                predicate: "normalize_price(price, decimals) >= 100.0".to_string(),
                action: "logger".to_string(),
                window_seconds: None,
                version: 1,
                enabled: true,
                state_ttl_seconds: None,
            },
            fuse_rule_core::config::RuleConfig {
                id: "domain_filter".to_string(),
                name: "Domain Filter".to_string(),
                predicate: "extract_domain(email) = 'example.com' OR extract_domain(url) = 'github.com'".to_string(),
                action: "logger".to_string(),
                window_seconds: None,
                version: 1,
                enabled: true,
                state_ttl_seconds: None,
            },
        ],
        sources: None,
    };

    let mut engine = RuleEngine::from_config(config).await?;

    // Register built-in UDFs
    let mut registry = UdfRegistry::new();
    BuiltinUdfs::register_all(&mut registry);
    
    println!("\n✅ Registered UDFs:");
    for udf_name in registry.list() {
        println!("  - {}", udf_name);
    }

    // Register UDFs to engine's context
    registry.register_to_context(engine.context())?;

    println!("\n🔄 Processing batch with UDF rules...\n");

    let traces = engine.process_batch(&batch).await?;

    println!("📈 Results:");
    for trace in &traces {
        if trace.action_fired {
            println!("\n  🔥 Rule Activated: {}", trace.rule_name);
            println!("     Predicate: {}", trace.predicate);
            println!("     Matched Rows: {}", trace.matched_row_count);
            println!("     Evaluation Time: {:?}", trace.evaluation_duration);
        }
    }

    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Summary");
    println!("=" .repeat(60));
    println!("Total rules evaluated: {}", traces.len());
    println!(
        "Rules activated: {}",
        traces.iter().filter(|t| t.action_fired).count()
    );
    println!(
        "Total matched rows: {}",
        traces.iter().map(|t| t.matched_row_count).sum::<usize>()
    );

    println!("\n✨ UDF Example Complete!\n");

    Ok(())
}
