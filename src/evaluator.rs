use anyhow::{Result, Context};
use arrow::record_batch::RecordBatch;
use arrow::array::Array;
use datafusion::prelude::*;
use datafusion::logical_expr::Expr;
use crate::rule::Rule;
use crate::state::PredicateResult;

pub struct DataFusionEvaluator {
    ctx: SessionContext,
}

impl DataFusionEvaluator {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub fn compile(&self, rule: Rule, schema: &arrow::datatypes::Schema) -> Result<CompiledPhysicalRule> {
        // DataFusion uses SQL-like expressions. 
        // We can parse the string predicate into an Expr.
        let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;
        
        let expr = self.ctx.parse_sql_expr(&rule.predicate, &df_schema)
            .context("Failed to parse rule predicate")?;
            
        Ok(CompiledPhysicalRule {
            rule,
            expr,
        })
    }

    pub async fn evaluate_batch(
        &self, 
        batch: &RecordBatch, 
        rules: &[CompiledPhysicalRule],
        window_batches: &[Vec<RecordBatch>] // One list of batches per rule window
    ) -> Result<Vec<(PredicateResult, Option<RecordBatch>)>> {
        let mut results = Vec::new();

        for (i, rule) in rules.iter().enumerate() {
            let active_batches = if rule.rule.window_seconds.is_some() {
                let mut all = window_batches[i].clone();
                all.push(batch.clone());
                all
            } else {
                vec![batch.clone()]
            };

            if active_batches.is_empty() {
                results.push((PredicateResult::False, None));
                continue;
            }

            // Register batches as a temporary table for this rule
            let table_name = format!("rule_input_{}", i);
            let df = self.ctx.read_batches(active_batches)?;
            self.ctx.register_table(&table_name, df.into_view())?;
            
            // Evaluate the predicate using SQL to let the planner handle aggregates/filters correctly
            let sql = format!("SELECT ({}) as match_result FROM {}", rule.rule.predicate, table_name);
            let select_df = self.ctx.sql(&sql).await?;
            let result_batches = select_df.collect().await?;
            
            let mut is_true = false;
            if !result_batches.is_empty() {
                let col = result_batches[0].column(0).as_any().downcast_ref::<arrow::array::BooleanArray>();
                if let Some(bool_col) = col {
                    if bool_col.len() > 0 && !bool_col.is_null(0) && bool_col.value(0) {
                        is_true = true;
                    }
                }
            }
            
            if is_true {
                results.push((PredicateResult::True, Some(batch.clone())));
            } else {
                results.push((PredicateResult::False, None));
            }
            
            // Clean up table
            self.ctx.deregister_table(&table_name)?;
        }

        Ok(results)
    }
}

pub struct CompiledPhysicalRule {
    pub rule: Rule,
    pub expr: Expr,
}

pub fn infer_json_schema(value: &serde_json::Value) -> arrow::datatypes::Schema {
    match value {
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                return arrow::datatypes::Schema::empty();
            }
            // Simple inference from first element
            let mut fields = Vec::new();
            if let Some(serde_json::Value::Object(map)) = arr.get(0) {
                for (k, v) in map {
                    let dt = match v {
                        serde_json::Value::Number(n) if n.is_i64() => arrow::datatypes::DataType::Int32, // Default to i32 for simplicity
                        serde_json::Value::Number(_) => arrow::datatypes::DataType::Float64,
                        serde_json::Value::Bool(_) => arrow::datatypes::DataType::Boolean,
                        _ => arrow::datatypes::DataType::Utf8,
                    };
                    fields.push(arrow::datatypes::Field::new(k, dt, true));
                }
            }
            arrow::datatypes::Schema::new(fields)
        }
        _ => arrow::datatypes::Schema::empty(),
    }
}
