//! User-Defined Functions (UDFs) for extending SQL predicates
//!
//! This module provides a registry and loader for custom functions that can be
//! used in rule predicates, extending the built-in SQL functions available in DataFusion.
//!
//! Note: Full UDF implementation requires DataFusion's ScalarUDFImpl trait which
//! has complex requirements. This provides the foundation structure that can be
//! extended with actual UDF implementations.

use anyhow::Result;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Registry for managing UDFs
#[derive(Clone)]
pub struct UdfRegistry {
    function_names: Vec<String>,
}

impl UdfRegistry {
    /// Create a new empty UDF registry
    pub fn new() -> Self {
        Self {
            function_names: Vec::new(),
        }
    }

    /// Register a UDF name (placeholder for future implementation)
    pub fn register_name(&mut self, name: String) {
        if !self.function_names.contains(&name) {
            self.function_names.push(name);
        }
    }

    /// Get all registered UDF names
    pub fn list(&self) -> Vec<String> {
        self.function_names.clone()
    }

    /// Register all UDFs in this registry to a DataFusion SessionContext
    /// This is a placeholder - actual UDF registration will be implemented
    /// when we have a proper ScalarUDFImpl implementation
    pub fn register_to_context(&self, _ctx: &SessionContext) -> Result<()> {
        // TODO: Implement actual UDF registration using ScalarUDFImpl
        // For now, DataFusion's built-in functions are sufficient
        Ok(())
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Built-in UDFs that come with FuseRule
pub struct BuiltinUdfs;

impl BuiltinUdfs {
    /// Register all built-in UDF names to a registry
    /// Note: Actual function implementations rely on DataFusion's built-in functions
    /// for now. Custom UDFs can be added later using ScalarUDFImpl.
    pub fn register_all(registry: &mut UdfRegistry) {
        // Register built-in function names
        // These are documented for users, but actual implementation
        // relies on DataFusion's built-in functions
        registry.register_name("abs".to_string());
        registry.register_name("round".to_string());
        registry.register_name("upper".to_string());
        registry.register_name("lower".to_string());
        registry.register_name("length".to_string());
        registry.register_name("is_empty".to_string());
        
        // Note: DataFusion already provides many of these functions.
        // Custom UDFs can be added by implementing ScalarUDFImpl trait.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_udf_registry() {
        let mut registry = UdfRegistry::new();
        registry.register_name("test_func".to_string());
        assert!(registry.list().contains(&"test_func".to_string()));
    }

    #[test]
    fn test_builtin_udfs() {
        let mut registry = UdfRegistry::new();
        BuiltinUdfs::register_all(&mut registry);
        let functions = registry.list();
        assert!(functions.contains(&"abs".to_string()));
        assert!(functions.contains(&"upper".to_string()));
    }
}
