use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeListArray, Float32Array, UInt64Array};
use arrow::compute;
use arrow::datatypes::DataType;
use futures_util::TryStreamExt;
use iceberg::{Catalog, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use likhadb_store::manager::CollectionManager;

use crate::error::LakehouseError;
use crate::parquet_io::build_payload;

/// Connection parameters for an Iceberg REST catalog backed by MinIO.
pub struct IcebergConfig {
    /// URI of the Iceberg REST catalog (e.g. `http://localhost:8181`).
    pub catalog_uri: String,
    /// MinIO/S3 endpoint for data-file access (e.g. `http://localhost:9000`).
    pub s3_endpoint: String,
    /// S3 access key.
    pub access_key: String,
    /// S3 secret key.
    pub secret_key: String,
    /// S3 region (`"us-east-1"` works for MinIO).
    pub region: String,
    /// Warehouse root URI (e.g. `s3://bucket/warehouse`).
    pub warehouse: String,
    /// Additional properties forwarded to the REST catalog.
    pub extra_properties: HashMap<String, String>,
}

/// Build a REST catalog pointing at an Iceberg server with MinIO as the data store.
///
/// The returned catalog uses path-style S3 requests so it works with a local
/// MinIO instance without virtual-host DNS configuration.
pub fn build_rest_catalog(config: &IcebergConfig) -> Result<RestCatalog, LakehouseError> {
    let mut props = HashMap::from([
        ("s3.endpoint".to_string(), config.s3_endpoint.clone()),
        ("s3.access-key-id".to_string(), config.access_key.clone()),
        ("s3.secret-access-key".to_string(), config.secret_key.clone()),
        ("s3.region".to_string(), config.region.clone()),
        ("s3.path-style-access".to_string(), "true".to_string()),
        ("warehouse".to_string(), config.warehouse.clone()),
    ]);
    props.extend(config.extra_properties.clone());

    let rest_config = RestCatalogConfig::builder()
        .uri(config.catalog_uri.clone())
        .props(props)
        .build();

    Ok(RestCatalog::new(rest_config))
}

/// Async Iceberg import trait for `CollectionManager`.
///
/// Mirrors the `LakehouseExt` and `ObjectStoreLakehouseExt` traits; the vector
/// schema constraints are identical:
///
/// - `id_col`: integer column cast to `UInt64`.
/// - `vector_col`: `FixedSizeList<Float32>` whose list size must match the
///   collection's declared dimension.
/// - `payload_cols`: columns merged into the per-vector JSON payload.
#[allow(async_fn_in_trait)]
pub trait IcebergLakehouseExt {
    /// Bulk-import vectors from the current snapshot of an Iceberg table.
    ///
    /// The table is addressed by `table_ident` inside `catalog`. All rows in the
    /// current snapshot are inserted into `collection_name`. Returns the number
    /// of vectors inserted.
    async fn import_iceberg(
        &mut self,
        collection_name: &str,
        catalog: &dyn Catalog,
        table_ident: &TableIdent,
        id_col: &str,
        vector_col: &str,
        payload_cols: &[&str],
    ) -> Result<usize, LakehouseError>;
}

impl IcebergLakehouseExt for CollectionManager {
    async fn import_iceberg(
        &mut self,
        collection_name: &str,
        catalog: &dyn Catalog,
        table_ident: &TableIdent,
        id_col: &str,
        vector_col: &str,
        payload_cols: &[&str],
    ) -> Result<usize, LakehouseError> {
        let collection_dim = {
            let col = self
                .get(collection_name)
                .map_err(|_| LakehouseError::CollectionNotFound(collection_name.to_string()))?;
            col.dim
        };

        let table = catalog
            .load_table(table_ident)
            .await
            .map_err(LakehouseError::Iceberg)?;

        let scan = table.scan().build().map_err(LakehouseError::Iceberg)?;
        let mut stream = scan.to_arrow().await.map_err(LakehouseError::Iceberg)?;

        let mut schema_validated = false;
        let mut batch_dim: usize = 0;
        let mut total: usize = 0;

        while let Some(batch) = stream.try_next().await.map_err(LakehouseError::Iceberg)? {
            let schema = batch.schema();

            // Validate schema once against the first batch.
            if !schema_validated {
                if schema.field_with_name(id_col).is_err() {
                    return Err(LakehouseError::ColumnNotFound(id_col.to_string()));
                }
                if schema.field_with_name(vector_col).is_err() {
                    return Err(LakehouseError::ColumnNotFound(vector_col.to_string()));
                }

                let vec_field = schema.field_with_name(vector_col).unwrap();
                batch_dim = match vec_field.data_type() {
                    DataType::FixedSizeList(inner, size) => {
                        if !matches!(inner.data_type(), DataType::Float32) {
                            return Err(LakehouseError::TypeMismatch {
                                col: vector_col.to_string(),
                                expected: "FixedSizeList<Float32>".to_string(),
                                got: vec_field.data_type().to_string(),
                            });
                        }
                        *size as usize
                    }
                    other => {
                        return Err(LakehouseError::TypeMismatch {
                            col: vector_col.to_string(),
                            expected: "FixedSizeList<Float32>".to_string(),
                            got: other.to_string(),
                        });
                    }
                };
                if batch_dim != collection_dim {
                    return Err(LakehouseError::DimMismatch {
                        expected: collection_dim,
                        got: batch_dim,
                    });
                }
                schema_validated = true;
            }

            let num_rows = batch.num_rows();

            let id_col_idx = schema
                .index_of(id_col)
                .map_err(|_| LakehouseError::ColumnNotFound(id_col.to_string()))?;
            let id_raw = batch.column(id_col_idx);
            let id_cast: Arc<dyn Array> = if id_raw.data_type() == &DataType::UInt64 {
                id_raw.clone()
            } else {
                compute::cast(id_raw, &DataType::UInt64)?
            };
            let id_array = id_cast
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    LakehouseError::Schema("id column could not be cast to UInt64".to_string())
                })?;

            let vec_col_idx = schema
                .index_of(vector_col)
                .map_err(|_| LakehouseError::ColumnNotFound(vector_col.to_string()))?;
            let vec_array = batch
                .column(vec_col_idx)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    LakehouseError::Schema("vector column is not FixedSizeListArray".to_string())
                })?;
            let float_values = vec_array
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    LakehouseError::Schema("vector values are not Float32".to_string())
                })?;

            let payload_col_indices: Vec<(usize, &str)> = payload_cols
                .iter()
                .map(|&name| {
                    schema
                        .index_of(name)
                        .map(|idx| (idx, name))
                        .map_err(|_| LakehouseError::ColumnNotFound(name.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let collection = self
                .get_mut(collection_name)
                .map_err(|_| LakehouseError::CollectionNotFound(collection_name.to_string()))?;

            for row in 0..num_rows {
                let id = id_array.value(row);
                let start = row * batch_dim;
                let end = start + batch_dim;
                let vec: Vec<f32> = float_values.values()[start..end].to_vec();
                let payload = build_payload(&batch, &payload_col_indices, row);
                collection.insert(id, vec, payload)?;
            }

            total += num_rows;
        }

        Ok(total)
    }
}
