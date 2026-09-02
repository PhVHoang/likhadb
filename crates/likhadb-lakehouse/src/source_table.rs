use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use likhadb_core::SourceBinding;

use crate::LakehouseError;

/// Convert a dependency-free source binding into its Iceberg table identifier.
pub fn source_table_ident(binding: &SourceBinding) -> Result<TableIdent, LakehouseError> {
    let namespace = NamespaceIdent::from_vec(binding.source_namespace.clone())
        .map_err(LakehouseError::Iceberg)?;
    Ok(TableIdent::new(namespace, binding.source_table.clone()))
}

/// Resolve and load the Iceberg table referenced by a source binding.
///
/// The catalog may be supplied by an injected `Arc<dyn Catalog>` via
/// `catalog.as_ref()`. Catalog failures are annotated with the fully-qualified
/// table name so a maintenance caller can log the affected binding and skip it.
pub async fn load_source_table(
    catalog: &dyn Catalog,
    binding: &SourceBinding,
) -> Result<Table, LakehouseError> {
    let ident = source_table_ident(binding)?;
    catalog
        .load_table(&ident)
        .await
        .map_err(|source| LakehouseError::SourceTableLoad {
            table: ident.to_string(),
            source: Box::new(source),
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{CatalogBuilder, ErrorKind, TableCreation};

    use super::*;

    fn binding(table: &str) -> SourceBinding {
        SourceBinding {
            source_namespace: vec!["source".to_string()],
            source_table: table.to_string(),
            id_column: "id".to_string(),
            vector_column: "vector".to_string(),
            payload_columns: vec![],
        }
    }

    fn source_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields([Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap()
    }

    async fn seeded_catalog() -> Arc<dyn Catalog> {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory://warehouse".to_string(),
                )]),
            )
            .await
            .unwrap();
        let namespace = NamespaceIdent::new("source".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("vectors".to_string())
                    .schema(source_schema())
                    .build(),
            )
            .await
            .unwrap();
        Arc::new(catalog)
    }

    #[test]
    fn maps_binding_to_table_ident() {
        let binding = SourceBinding {
            source_namespace: vec!["lake".to_string(), "embeddings".to_string()],
            source_table: "documents".to_string(),
            id_column: "id".to_string(),
            vector_column: "vector".to_string(),
            payload_columns: vec![],
        };

        let ident = source_table_ident(&binding).unwrap();

        assert_eq!(ident.namespace().as_ref(), &["lake", "embeddings"]);
        assert_eq!(ident.name(), "documents");
    }

    #[tokio::test]
    async fn loads_seeded_source_table() {
        let catalog = seeded_catalog().await;

        let table = load_source_table(catalog.as_ref(), &binding("vectors"))
            .await
            .unwrap();

        assert_eq!(table.identifier().to_string(), "source.vectors");
    }

    #[tokio::test]
    async fn reports_missing_source_table() {
        let catalog = seeded_catalog().await;

        let error = load_source_table(catalog.as_ref(), &binding("missing"))
            .await
            .unwrap_err();

        match &error {
            LakehouseError::SourceTableLoad { table, source } => {
                assert_eq!(table, "source.missing");
                assert_eq!(source.kind(), ErrorKind::TableNotFound);
            }
            other => panic!("unexpected error: {other}"),
        }
        assert!(error.to_string().contains("source.missing"));
    }
}
