use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Array, FixedSizeListArray, StringArray, UInt64Array, UInt64Builder,
};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use likhadb_persist::WalManager;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::Value;

use crate::error::LakehouseError;
use crate::parquet_io::{build_payload, LakehouseExt};

const ROW_GROUP_SIZE: usize = 65_536;

impl LakehouseExt for WalManager {
    fn export_parquet(
        &self,
        collection_name: &str,
        path: &Path,
    ) -> Result<(), LakehouseError> {
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

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![id_array, vector_array, payload_array],
            )?;
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
            let id_array_cast = if id_array_raw.data_type() == &DataType::UInt64 {
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
                    LakehouseError::Schema(
                        "vector column is not FixedSizeListArray".to_string(),
                    )
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
                    batch
                        .schema()
                        .index_of(name)
                        .map(|idx| (idx, name))
                        .map_err(|_| LakehouseError::ColumnNotFound(name.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;

            for row in 0..num_rows {
                let id = id_array.value(row);
                let start = row * collection_dim;
                let end = start + collection_dim;
                let vec: Vec<f32> = float_values.values()[start..end].to_vec();
                let payload = build_payload(&batch, &payload_col_indices, row);
                self.insert(collection_name, id, vec, payload)
                    .map_err(|e| LakehouseError::Schema(e.to_string()))?;
            }

            total += num_rows;
        }

        Ok(total)
    }
}
