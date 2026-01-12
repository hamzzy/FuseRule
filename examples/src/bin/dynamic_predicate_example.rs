use anyhow::Result;
use fuse_rule_core::rule_graph::dynamic_predicate::{DynamicPredicate, PredicateLoader, PredicateSource};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("🌐 FuseRule Dynamic Predicate Example\n");
    println!("This example demonstrates loading predicates from external sources");
    println!("including HTTP endpoints, databases, and S3 buckets\n");

    let loader = PredicateLoader::new();

    // Example 1: Static Predicate with A/B Testing
    println!("=" .repeat(60));
    println!("Example 1: A/B Testing with Multiple Versions");
    println!("=" .repeat(60));

    let mut dynamic_pred = DynamicPredicate {
        rule_id: "price_threshold".to_string(),
        source: PredicateSource::Static {
            predicate: "price > 100".to_string(),
        },
        versions: HashMap::new(),
        active_version: Some("v1".to_string()),
        feature_flags: HashMap::new(),
        refresh_interval_seconds: None,
    };

    // Add multiple versions for A/B testing
    dynamic_pred.add_version("v1".to_string(), "price > 100".to_string());
    dynamic_pred.add_version("v2".to_string(), "price > 200".to_string());
    dynamic_pred.add_version("v3".to_string(), "price > 150 AND volume > 1000".to_string());

    println!("\n📋 Available Versions:");
    for (version_id, predicate) in &dynamic_pred.versions {
        println!("  {} -> {}", version_id, predicate);
    }

    println!("\n🎯 Active Version: {}", dynamic_pred.active_version.as_ref().unwrap());
    println!("   Predicate: {}", dynamic_pred.get_predicate_for_tenant(None).unwrap());

    // Example 2: Feature Flags for Multi-Tenant
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Example 2: Multi-Tenant Feature Flags");
    println!("=" .repeat(60));

    // Set feature flags for specific tenants
    dynamic_pred.set_feature_flag("tenant_premium".to_string(), "v3".to_string());
    dynamic_pred.set_feature_flag("tenant_beta".to_string(), "v2".to_string());

    println!("\n🏢 Tenant-Specific Predicates:");
    
    let tenants = vec![
        ("tenant_premium", Some("tenant_premium")),
        ("tenant_beta", Some("tenant_beta")),
        ("tenant_standard", None),
    ];

    for (name, tenant_id) in tenants {
        let predicate = dynamic_pred.get_predicate_for_tenant(tenant_id).unwrap();
        println!("  {} -> {}", name, predicate);
    }

    // Register the dynamic predicate
    loader.register(dynamic_pred.clone()).await;

    // Example 3: HTTP Source Configuration
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Example 3: HTTP Predicate Source");
    println!("=" .repeat(60));

    let http_predicate = DynamicPredicate {
        rule_id: "http_rule".to_string(),
        source: PredicateSource::Http {
            url: "https://api.example.com/rules/price_threshold".to_string(),
            auth_header: Some("Bearer token123".to_string()),
        },
        versions: HashMap::new(),
        active_version: None,
        feature_flags: HashMap::new(),
        refresh_interval_seconds: Some(300), // Refresh every 5 minutes
    };

    println!("\n📡 HTTP Configuration:");
    if let PredicateSource::Http { url, auth_header } = &http_predicate.source {
        println!("  URL: {}", url);
        println!("  Auth: {}", if auth_header.is_some() { "Configured" } else { "None" });
        println!("  Refresh Interval: {} seconds", http_predicate.refresh_interval_seconds.unwrap());
    }

    println!("\n📝 Expected Response Format:");
    println!(r#"
    {{
      "rule_id": "http_rule",
      "versions": {{
        "v1": "price > 100",
        "v2": "price > 200"
      }},
      "active_version": "v1",
      "feature_flags": {{
        "tenant_123": "v2"
      }}
    }}
    "#);

    // Example 4: Database Source Configuration
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Example 4: Database Predicate Source");
    println!("=" .repeat(60));

    let db_predicate = DynamicPredicate {
        rule_id: "db_rule".to_string(),
        source: PredicateSource::Database {
            connection_string: "http://localhost:8123".to_string(),
            table: "rule_predicates".to_string(),
            predicate_column: "predicate".to_string(),
            rule_id_column: "rule_id".to_string(),
        },
        versions: HashMap::new(),
        active_version: None,
        feature_flags: HashMap::new(),
        refresh_interval_seconds: Some(600), // Refresh every 10 minutes
    };

    println!("\n🗄️  Database Configuration:");
    if let PredicateSource::Database {
        connection_string,
        table,
        predicate_column,
        rule_id_column,
    } = &db_predicate.source
    {
        println!("  Connection: {}", connection_string);
        println!("  Table: {}", table);
        println!("  Predicate Column: {}", predicate_column);
        println!("  Rule ID Column: {}", rule_id_column);
    }

    println!("\n📝 Expected Table Schema:");
    println!("  CREATE TABLE rule_predicates (");
    println!("    rule_id String,");
    println!("    version_id String,");
    println!("    predicate String");
    println!("  )");

    // Example 5: S3 Source Configuration
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Example 5: S3 Predicate Source");
    println!("=" .repeat(60));

    let s3_predicate = DynamicPredicate {
        rule_id: "s3_rule".to_string(),
        source: PredicateSource::S3 {
            bucket: "my-rules-bucket".to_string(),
            key: "rules/price_threshold.json".to_string(),
            region: Some("us-east-1".to_string()),
        },
        versions: HashMap::new(),
        active_version: None,
        feature_flags: HashMap::new(),
        refresh_interval_seconds: Some(900), // Refresh every 15 minutes
    };

    println!("\n☁️  S3 Configuration:");
    if let PredicateSource::S3 { bucket, key, region } = &s3_predicate.source {
        println!("  Bucket: {}", bucket);
        println!("  Key: {}", key);
        println!("  Region: {}", region.as_ref().unwrap());
    }

    // Example 6: Predicate Retrieval
    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Example 6: Retrieving Predicates");
    println!("=" .repeat(60));

    let predicate = loader.get_predicate("price_threshold", None).await;
    println!("\n🔍 Retrieved Predicate:");
    println!("  Rule ID: price_threshold");
    println!("  Tenant: default");
    println!("  Predicate: {}", predicate.unwrap());

    let premium_predicate = loader
        .get_predicate("price_threshold", Some("tenant_premium"))
        .await;
    println!("\n🔍 Retrieved Predicate:");
    println!("  Rule ID: price_threshold");
    println!("  Tenant: tenant_premium");
    println!("  Predicate: {}", premium_predicate.unwrap());

    println!("\n" .repeat(2));
    println!("=" .repeat(60));
    println!("Summary");
    println!("=" .repeat(60));
    println!("✅ Demonstrated Features:");
    println!("  - A/B testing with multiple predicate versions");
    println!("  - Multi-tenant feature flags");
    println!("  - HTTP endpoint integration");
    println!("  - Database (ClickHouse) integration");
    println!("  - S3 bucket integration");
    println!("  - Automatic refresh intervals");

    println!("\n💡 Use Cases:");
    println!("  - Deploy rule changes without restarting");
    println!("  - Test new rules with specific tenants");
    println!("  - Centralize rule management in databases");
    println!("  - Version control rules in S3");
    println!("  - Gradual rollout of rule changes");

    println!("\n✨ Dynamic Predicate Example Complete!\n");

    Ok(())
}
