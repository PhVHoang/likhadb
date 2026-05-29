use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, StringArray, UInt64Array, UInt64Builder,
};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::Value;

use crate::error::LakehouseError;
use crate::parquet_io::build_payload;
use likhadb_store::manager::CollectionManager;

const ROW_GROUP_SIZE: usize = 65_536;

/// Connection parameters for a MinIO (or any S3-compatible) object store.
pub struct MinioConfig {
    /// Full URL of the MinIO server, e.g. `"http://localhost:9000"`.
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// Region string; MinIO accepts any value — use `"us-east-1"` if unsure.
    pub region: String,
}

/// Build an `ObjectStore` handle pointing at a MinIO bucket.
///
/// Uses path-style requests (`/<bucket>/key`) required by local MinIO.
pub fn build_minio_store(config: &MinioConfig) -> Result<Arc<dyn ObjectStore>, LakehouseError> {
    let store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(&config.endpoint)
        .with_bucket_name(&config.bucket)
        .with_access_key_id(&config.access_key)
        .with_secret_access_key(&config.secret_key)
        .with_region(&config.region)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .map_err(LakehouseError::ObjectStore)?;
    Ok(Arc::new(store))
}

/// Async Parquet import/export backed by any `object_store::ObjectStore`.
///
/// The Parquet schema and column semantics are identical to `LakehouseExt`.
#[allow(async_fn_in_trait)]
pub trait ObjectStoreLakehouseExt {
    /// Serialize `collection_name` to Parquet bytes and upload to `path` in `store`.
    async fn export_parquet_to_store(
        &self,
        collection_name: &str,
        store: &Arc<dyn ObjectStore>,
        path: &StorePath,
    ) -> Result<(), LakehouseError>;

    /// Download Parquet from `path` in `store` and import into `collection_name`.
    ///
    /// - `id_col`: column for vector IDs (UInt64 or auto-castable integer).
    /// - `vector_col`: `FixedSizeList<Float32>` column.
    /// - `payload_cols`: columns merged into the vector's JSON payload.
    ///
    /// Returns the number of vectors imported.
    async fn import_parquet_from_store(
        &mut self,
        collection_name: &str,
        store: &Arc<dyn ObjectStore>,
        path: &StorePath,
        id_col: &str,
        vector_col: &str,
        payload_cols: &[&str],
    ) -> Result<usize, LakehouseError>;
}

impl ObjectStoreLakehouseExt for CollectionManager {
    async fn export_parquet_to_store(
        &self,
        collection_name: &str,
        store: &Arc<dyn ObjectStore>,
        path: &StorePath,
    ) -> Result<(), LakehouseError> {
        let bytes = collection_to_parquet_bytes(self, collection_name)?;
        store
            .put(path, bytes.into())
            .await
            .map_err(LakehouseError::ObjectStore)?;
        Ok(())
    }

    async fn import_parquet_from_store(
        &mut self,
        collection_name: &str,
        store: &Arc<dyn ObjectStore>,
        path: &StorePath,
        id_col: &str,
        vector_col: &str,
        payload_cols: &[&str],
    ) -> Result<usize, LakehouseError> {
        let bytes = store
            .get(path)
            .await
            .map_err(LakehouseError::ObjectStore)?
            .bytes()
            .await
            .map_err(LakehouseError::ObjectStore)?;
        parquet_bytes_into_collection(self, collection_name, bytes, id_col, vector_col, payload_cols)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn collection_to_parquet_bytes(
    manager: &CollectionManager,
    collection_name: &str,
) -> Result<Bytes, LakehouseError> {
    let collection = manager
        .get(collection_name)
        .map_err(|_| LakehouseError::CollectionNotFound(collection_name.to_string()))?;

    let dim = collection.dim;
    let ids = collection.list_ids();

    let vector_item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(vector_item_field.clone(), dim as i32),
            false,
        ),
        Field::new("payload", DataType::Utf8, true),
    ]));

    let mut buf: Vec<u8> = Vec::new();
    {
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_size(ROW_GROUP_SIZE)
            .build();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))?;

        for chunk in ids.chunks(ROW_GROUP_SIZE) {
            let mut id_builder = UInt64Builder::with_capacity(chunk.len());
            let mut float_values: Vec<f32> = Vec::with_capacity(chunk.len() * dim);
            let mut payload_strings: Vec<Option<String>> = Vec::with_capacity(chunk.len());

            for &id in chunk {
                if let Ok(Some((vec, payload_opt))) = collection.get(id) {
                    id_builder.append_value(id);
                    float_values.extend_from_slice(&vec);
                    payload_strings.push(payload_opt.as_ref().map(Value::to_string));
                }
            }

            let id_array: ArrayRef = Arc::new(id_builder.finish());
            let float_array = Arc::new(Float32Array::from(float_values));
            let vector_array: ArrayRef = Arc::new(FixedSizeListArray::try_new(
                vector_item_field.clone(),
                dim as i32,
                float_array,
                None,
            )?);
            let payload_array: ArrayRef = Arc::new(StringArray::from(payload_strings));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![id_array, vector_array, payload_array],
            )?;
            writer.write(&batch)?;
        }

        writer.close()?;
    }

    Ok(Bytes::from(buf))
}

fn parquet_bytes_into_collection(
    manager: &mut CollectionManager,
    collection_name: &str,
    bytes: Bytes,
    id_col: &str,
    vector_col: &str,
    payload_cols: &[&str],
) -> Result<usize, LakehouseError> {
    let collection_dim = {
        let col = manager
            .get(collection_name)
            .map_err(|_| LakehouseError::CollectionNotFound(collection_name.to_string()))?;
        col.dim
    };

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let schema = builder.schema().clone();

    if schema.field_with_name(id_col).is_err() {
        return Err(LakehouseError::ColumnNotFound(id_col.to_string()));
    }
    if schema.field_with_name(vector_col).is_err() {
        return Err(LakehouseError::ColumnNotFound(vector_col.to_string()));
    }

    let vec_field = schema.field_with_name(vector_col).unwrap();
    let parquet_dim = match vec_field.data_type() {
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

    if parquet_dim != collection_dim {
        return Err(LakehouseError::DimMismatch {
            expected: collection_dim,
            got: parquet_dim,
        });
    }

    let reader = builder.build()?;
    let mut total: usize = 0;

    for batch_result in reader {
        let batch = batch_result?;
        let num_rows = batch.num_rows();

        let id_col_idx = batch
            .schema()
            .index_of(id_col)
            .map_err(|_| LakehouseError::ColumnNotFound(id_col.to_string()))?;
        let id_array_raw = batch.column(id_col_idx);
        let id_array_cast: Arc<dyn Array> = if id_array_raw.data_type() == &DataType::UInt64 {
            id_array_raw.clone()
        } else {
            compute::cast(id_array_raw, &DataType::UInt64)?
        };
        let id_array = id_array_cast
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                LakehouseError::Schema("id column could not be cast to UInt64".to_string())
            })?;

        let vec_col_idx = batch
            .schema()
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
            .ok_or_else(|| LakehouseError::Schema("vector values are not Float32".to_string()))?;

        let payload_col_indices: Vec<(usize, &str)> = payload_cols
            .iter()
            .map(|&name| {
                batch
                    .schema()
                    .index_of(name)
                    .map(|idx| (idx, name))
                    .map_err(|_| LakehouseError::ColumnNotFound(name.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let collection = manager
            .get_mut(collection_name)
            .map_err(|_| LakehouseError::CollectionNotFound(collection_name.to_string()))?;

        for row in 0..num_rows {
            let id = id_array.value(row);
            let start = row * collection_dim;
            let end = start + collection_dim;
            let vec: Vec<f32> = float_values.values()[start..end].to_vec();
            let payload = build_payload(&batch, &payload_col_indices, row);
            collection.insert(id, vec, payload)?;
        }

        total += num_rows;
    }

    Ok(total)
}
