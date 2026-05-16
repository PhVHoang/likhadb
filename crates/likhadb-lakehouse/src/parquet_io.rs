use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int32Array,
    Int64Array, StringArray, UInt32Array, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use likhadb_store::manager::CollectionManager;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::{Map, Value};

use crate::error::LakehouseError;

const ROW_GROUP_SIZE: usize = 65_536;

pub trait LakehouseExt {
    /// Export a collection to a Parquet file.
    ///
    /// Output schema: `id: UInt64`, `vector: FixedSizeList<Float32>[dim]`,
    /// `payload: Utf8` (nullable, JSON-serialised payload).
    fn export_parquet(&self, collection_name: &str, path: &Path) -> Result<(), LakehouseError>;

    /// Import vectors from a Parquet file into an existing collection.
    ///
    /// - `id_col`: column for vector IDs (UInt64 or auto-castable integer).
    /// - `vector_col`: column for vectors (`FixedSizeList<Float32>`).
    /// - `payload_cols`: columns to merge into the vector payload JSON. A single
    ///   column named `"payload"` whose values are JSON strings is parsed directly
    ///   as the payload `Value` (enables lossless export→import round-trips).
    ///
    /// Returns the number of vectors imported.
    fn import_parquet(
        &mut self,
        collection_name: &str,
        path: &Path,
        id_col: &str,
        vector_col: &str,
        payload_cols: &[&str],
    ) -> Result<usize, LakehouseError>;
}

impl LakehouseExt for CollectionManager {
    fn export_parquet(&self, collection_name: &str, path: &Path) -> Result<(), LakehouseError> {
        let collection = self
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

        let file = std::fs::File::create(path)?;
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_size(ROW_GROUP_SIZE)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

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

            let batch =
                RecordBatch::try_new(schema.clone(), vec![id_array, vector_array, payload_array])?;
            writer.write(&batch)?;
        }

        writer.close()?;
        Ok(())
    }

    fn import_parquet(
        &mut self,
        collection_name: &str,
        path: &Path,
        id_col: &str,
        vector_col: &str,
        payload_cols: &[&str],
    ) -> Result<usize, LakehouseError> {
        // Scope the immutable borrow so we can borrow mutably per batch later.
        let collection_dim = {
            let col = self
                .get(collection_name)
                .map_err(|_| LakehouseError::CollectionNotFound(collection_name.to_string()))?;
            col.dim
        };

        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();

        if schema.field_with_name(id_col).is_err() {
            return Err(LakehouseError::ColumnNotFound(id_col.to_string()));
        }
        if schema.field_with_name(vector_col).is_err() {
            return Err(LakehouseError::ColumnNotFound(vector_col.to_string()));
        }

        // Validate vector column type and extract embedded dim.
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
                arrow::compute::cast(id_array_raw, &DataType::UInt64)?
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
                .ok_or_else(|| {
                    LakehouseError::Schema("vector values are not Float32".to_string())
                })?;

            // Resolve payload column indices once per batch.
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

            let collection = self
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
}

pub(crate) fn build_payload(
    batch: &RecordBatch,
    payload_col_indices: &[(usize, &str)],
    row: usize,
) -> Option<Value> {
    if payload_col_indices.is_empty() {
        return None;
    }

    // Single column named "payload" containing a JSON string → parse directly for round-trip.
    if payload_col_indices.len() == 1 {
        let (idx, name) = payload_col_indices[0];
        if name == "payload" {
            if let Some(arr) = batch.column(idx).as_any().downcast_ref::<StringArray>() {
                if !arr.is_null(row) {
                    let s = arr.value(row);
                    if let Ok(v) = serde_json::from_str::<Value>(s) {
                        return Some(v);
                    }
                }
            }
        }
    }

    let mut map = Map::new();
    for &(idx, name) in payload_col_indices {
        let col = batch.column(idx);
        if col.is_null(row) {
            continue;
        }
        if let Some(v) = col_value_at(col.as_ref(), row) {
            map.insert(name.to_string(), v);
        }
    }

    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

fn col_value_at(col: &dyn Array, row: usize) -> Option<Value> {
    let any = col.as_any();
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return Some(Value::String(a.value(row).to_string()));
    }
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Some(Value::Number(a.value(row).into()));
    }
    if let Some(a) = any.downcast_ref::<Int32Array>() {
        return Some(Value::Number(a.value(row).into()));
    }
    if let Some(a) = any.downcast_ref::<UInt64Array>() {
        return Some(Value::Number(a.value(row).into()));
    }
    if let Some(a) = any.downcast_ref::<UInt32Array>() {
        return Some(Value::Number(a.value(row).into()));
    }
    if let Some(a) = any.downcast_ref::<Float32Array>() {
        let v = a.value(row) as f64;
        return serde_json::Number::from_f64(v).map(Value::Number);
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        let v = a.value(row);
        return serde_json::Number::from_f64(v).map(Value::Number);
    }
    if let Some(a) = any.downcast_ref::<BooleanArray>() {
        return Some(Value::Bool(a.value(row)));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use likhadb_core::Metric;
    use tempfile::tempdir;

    fn make_manager_with_collection(name: &str, dim: usize) -> CollectionManager {
        let mut m = CollectionManager::new();
        m.create_collection(name, dim, Metric::L2).unwrap();
        m
    }

    #[test]
    fn test_export_schema() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("out.parquet");

        let mut m = make_manager_with_collection("c", 4);
        let col = m.get_mut("c").unwrap();
        col.insert(
            1,
            vec![1.0, 2.0, 3.0, 4.0],
            Some(serde_json::json!({"tag": "a"})),
        )
        .unwrap();
        col.insert(2, vec![5.0, 6.0, 7.0, 8.0], None).unwrap();

        m.export_parquet("c", &path).unwrap();
        assert!(path.exists());

        let file = std::fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema = builder.schema();

        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            schema.field_with_name("payload").unwrap().data_type(),
            &DataType::Utf8
        );
        let vec_field = schema.field_with_name("vector").unwrap();
        assert!(matches!(
            vec_field.data_type(),
            DataType::FixedSizeList(_, 4)
        ));
    }

    #[test]
    fn test_export_null_payload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("out.parquet");

        let mut m = make_manager_with_collection("c", 2);
        let col = m.get_mut("c").unwrap();
        col.insert(1, vec![1.0, 2.0], None).unwrap();
        col.insert(2, vec![3.0, 4.0], Some(serde_json::json!({"x": 1})))
            .unwrap();

        m.export_parquet("c", &path).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        let payload_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let null_count = (0..payload_col.len())
            .filter(|&i| payload_col.is_null(i))
            .count();
        assert_eq!(null_count, 1);
    }

    #[test]
    fn test_import_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("import.parquet");

        let vector_field = Arc::new(Field::new("item", DataType::Float32, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(vector_field.clone(), 3),
                false,
            ),
            Field::new("payload", DataType::Utf8, true),
        ]));

        let id_array: ArrayRef = Arc::new(UInt64Array::from(vec![10u64, 20, 30]));
        let float_array = Arc::new(Float32Array::from(vec![
            1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let vec_array: ArrayRef =
            Arc::new(FixedSizeListArray::try_new(vector_field, 3, float_array, None).unwrap());
        let payload_array: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"label":"a"}"#),
            None,
            Some(r#"{"label":"c"}"#),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![id_array, vec_array, payload_array]).unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut m = make_manager_with_collection("dst", 3);
        let count = m
            .import_parquet("dst", &path, "id", "vector", &["payload"])
            .unwrap();
        assert_eq!(count, 3);

        let col = m.get("dst").unwrap();
        assert_eq!(col.len(), 3);
        let (vec, payload) = col.get(10).unwrap().unwrap();
        assert_eq!(vec, vec![1.0, 2.0, 3.0]);
        assert_eq!(payload.unwrap()["label"], "a");
    }

    #[test]
    fn test_import_type_coercion() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("coerce.parquet");

        let vector_field = Arc::new(Field::new("item", DataType::Float32, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(vector_field.clone(), 2),
                false,
            ),
        ]));

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![100i64, 200]));
        let float_array = Arc::new(Float32Array::from(vec![1.0f32, 2.0, 3.0, 4.0]));
        let vec_array: ArrayRef =
            Arc::new(FixedSizeListArray::try_new(vector_field, 2, float_array, None).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, vec_array]).unwrap();

        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut m = make_manager_with_collection("dst", 2);
        let count = m.import_parquet("dst", &path, "id", "vector", &[]).unwrap();
        assert_eq!(count, 2);
        assert!(m.get("dst").unwrap().get(100).unwrap().is_some());
        assert!(m.get("dst").unwrap().get(200).unwrap().is_some());
    }

    #[test]
    fn test_import_missing_column_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("missing.parquet");

        let vector_field = Arc::new(Field::new("item", DataType::Float32, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(vector_field.clone(), 2),
                false,
            ),
        ]));

        let id_array: ArrayRef = Arc::new(UInt64Array::from(vec![1u64]));
        let float_array = Arc::new(Float32Array::from(vec![1.0f32, 2.0]));
        let vec_array: ArrayRef =
            Arc::new(FixedSizeListArray::try_new(vector_field, 2, float_array, None).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, vec_array]).unwrap();

        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut m = make_manager_with_collection("dst", 2);
        let err = m
            .import_parquet("dst", &path, "id", "no_such_col", &[])
            .unwrap_err();
        assert!(matches!(err, LakehouseError::ColumnNotFound(_)));
    }

    #[test]
    fn test_import_dim_mismatch_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dim.parquet");

        let vector_field = Arc::new(Field::new("item", DataType::Float32, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(vector_field.clone(), 3),
                false,
            ),
        ]));

        let id_array: ArrayRef = Arc::new(UInt64Array::from(vec![1u64]));
        let float_array = Arc::new(Float32Array::from(vec![1.0f32, 2.0, 3.0]));
        let vec_array: ArrayRef =
            Arc::new(FixedSizeListArray::try_new(vector_field, 3, float_array, None).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, vec_array]).unwrap();

        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut m = make_manager_with_collection("dst", 4); // dim=4, file has dim=3
        let err = m
            .import_parquet("dst", &path, "id", "vector", &[])
            .unwrap_err();
        assert!(matches!(
            err,
            LakehouseError::DimMismatch {
                expected: 4,
                got: 3
            }
        ));
    }
}
