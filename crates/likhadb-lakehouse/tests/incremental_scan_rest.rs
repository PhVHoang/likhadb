//! End-to-end validation of `scan_delta` against a REST catalog backed by MinIO.
//!
//! Start the external services as documented in `iceberg_round_trip.rs`, then run:
//! `cargo test -p likhadb-lakehouse --features iceberg --test incremental_scan_rest -- --ignored --nocapture`

#![cfg(feature = "iceberg")]

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, Operation, Snapshot, SnapshotReference, SnapshotRetention, Struct,
    Summary, MAIN_BRANCH,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate};
use iceberg_catalog_rest::CommitTableRequest;
use likhadb_core::SourceBinding;
use likhadb_lakehouse::{
    build_rest_catalog, scan_delta, IcebergConfig, LakehouseError, SnapshotDelta,
};
use likhadb_store::DeltaRow;
use parquet::arrow::ArrowWriter;

const DIM: i32 = 2;

fn field(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        "PARQUET:field_id".to_string(),
        id.to_string(),
    )]))
}

fn arrow_schema() -> Arc<ArrowSchema> {
    // Iceberg assigns top-level fields before nested list elements.
    let element = Arc::new(field("element", DataType::Float32, false, 4));
    Arc::new(ArrowSchema::new(vec![
        field("id", DataType::Int64, false, 1),
        field("embedding", DataType::FixedSizeList(element, DIM), false, 2),
        field("payload", DataType::Utf8, true, 3),
    ]))
}

fn config() -> IcebergConfig {
    let bucket = std::env::var("MINIO_BUCKET").expect("MINIO_BUCKET required");
    IcebergConfig {
        catalog_uri: std::env::var("ICEBERG_CATALOG_URI").expect("ICEBERG_CATALOG_URI required"),
        s3_endpoint: std::env::var("MINIO_ENDPOINT").expect("MINIO_ENDPOINT required"),
        access_key: std::env::var("MINIO_ACCESS_KEY").expect("MINIO_ACCESS_KEY required"),
        secret_key: std::env::var("MINIO_SECRET_KEY").expect("MINIO_SECRET_KEY required"),
        region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        warehouse: format!("s3://{bucket}/warehouse"),
        extra_properties: HashMap::new(),
    }
}

fn unique_namespace() -> NamespaceIdent {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before Unix epoch")
        .as_nanos();
    NamespaceIdent::new(format!(
        "likhadb_scan_delta_{}_{}",
        std::process::id(),
        nanos
    ))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before Unix epoch")
        .as_millis() as i64
}

fn new_snapshot_id() -> i64 {
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before Unix epoch")
        .as_nanos()
        & i64::MAX as u128) as i64
}

fn parquet_bytes(
    ids: &[i64],
    vectors: &[[f32; DIM as usize]],
    payloads: &[&str],
) -> Result<Vec<u8>, Box<dyn Error>> {
    let schema = arrow_schema();
    let id_array: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
    let values = vectors
        .iter()
        .flat_map(|vector| vector.iter().copied())
        .collect::<Vec<_>>();
    let vector_array: ArrayRef = Arc::new(FixedSizeListArray::try_new(
        Arc::new(field("element", DataType::Float32, false, 4)),
        DIM,
        Arc::new(Float32Array::from(values)),
        None,
    )?);
    let payload_array: ArrayRef = Arc::new(StringArray::from(payloads.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, vector_array, payload_array])?;

    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

async fn append_rows(
    catalog: &dyn Catalog,
    table: &iceberg::table::Table,
    file_name: &str,
    ids: &[i64],
    vectors: &[[f32; DIM as usize]],
    payloads: &[&str],
) -> Result<(iceberg::table::Table, DataFile), Box<dyn Error>> {
    let parquet = parquet_bytes(ids, vectors, payloads)?;
    let file_path = format!("{}/data/{file_name}.parquet", table.metadata().location());
    table
        .file_io()
        .new_output(&file_path)?
        .write(Bytes::from(parquet.clone()))
        .await?;

    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(file_path)
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .record_count(ids.len() as u64)
        .file_size_in_bytes(parquet.len() as u64)
        .build()?;
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files([data_file.clone()])
        .apply(tx)?;
    Ok((tx.commit(catalog).await?, data_file))
}

async fn commit_file_drop(
    catalog_uri: &str,
    table: &iceberg::table::Table,
    dropped_file: &DataFile,
) -> Result<i64, Box<dyn Error>> {
    let parent = table
        .metadata()
        .current_snapshot()
        .expect("S1 snapshot exists");
    let parent_id = parent.snapshot_id();
    let sequence_number = table.metadata().next_sequence_number();
    let snapshot_id = new_snapshot_id();
    let old_manifest_list = parent
        .load_manifest_list(table.file_io(), table.metadata())
        .await?;

    let mut retained_manifests = Vec::<ManifestFile>::new();
    let mut dropped_entry = None;
    for manifest_file in old_manifest_list.entries() {
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        if let Some(entry) = manifest
            .entries()
            .iter()
            .find(|entry| entry.file_path() == dropped_file.file_path())
        {
            assert_eq!(
                manifest.entries().len(),
                1,
                "test append should create one file per manifest"
            );
            dropped_entry = Some(entry.clone());
        } else {
            retained_manifests.push(manifest_file.clone());
        }
    }
    let dropped_entry = dropped_entry.expect("S0 data file is in the S1 manifest list");

    let manifest_path = format!(
        "{}/metadata/scan_delta_{}_delete.avro",
        table.metadata().location(),
        snapshot_id
    );
    let output = table.file_io().new_output(&manifest_path)?;
    let mut manifest_writer = ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        None,
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    manifest_writer.add_delete_file(
        dropped_entry.data_file().clone(),
        dropped_entry
            .sequence_number()
            .expect("committed file has a data sequence number"),
        dropped_entry.file_sequence_number,
    )?;
    retained_manifests.push(manifest_writer.write_manifest_file().await?);

    let manifest_list_path = format!(
        "{}/metadata/snap-{}-scan_delta.avro",
        table.metadata().location(),
        snapshot_id
    );
    let output = table.file_io().new_output(&manifest_list_path)?;
    let mut manifest_list_writer =
        ManifestListWriter::v2(output, snapshot_id, Some(parent_id), sequence_number);
    manifest_list_writer.add_manifests(retained_manifests.into_iter())?;
    manifest_list_writer.close().await?;

    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(Some(parent_id))
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(now_ms())
        .with_manifest_list(manifest_list_path)
        .with_summary(Summary {
            operation: Operation::Delete,
            additional_properties: HashMap::from([
                ("deleted-data-files".to_string(), "1".to_string()),
                (
                    "deleted-records".to_string(),
                    dropped_file.record_count().to_string(),
                ),
            ]),
        })
        .with_schema_id(table.metadata().current_schema_id())
        .build();
    let request = CommitTableRequest {
        identifier: Some(table.identifier().clone()),
        requirements: vec![
            TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(parent_id),
            },
        ],
        updates: vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ],
    };
    let endpoint = format!(
        "{}/v1/namespaces/{}/tables/{}",
        catalog_uri.trim_end_matches('/'),
        table.identifier().namespace().to_url_string(),
        table.identifier().name()
    );
    let response = reqwest::Client::new()
        .post(endpoint)
        .json(&request)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    assert!(status.is_success(), "REST commit failed ({status}): {body}");
    Ok(snapshot_id)
}

fn binding(namespace: &NamespaceIdent, table: &TableIdent) -> SourceBinding {
    SourceBinding {
        source_namespace: namespace.as_ref().to_vec(),
        source_table: table.name().to_string(),
        id_column: "id".to_string(),
        vector_column: "embedding".to_string(),
        payload_columns: vec!["payload".to_string()],
    }
}

fn assert_upsert(row: &DeltaRow, expected_id: u64, expected_vector: &[f32]) {
    match row {
        DeltaRow::Upsert {
            id,
            vector,
            payload,
        } => {
            assert_eq!(*id, expected_id);
            assert_eq!(vector, expected_vector);
            assert_eq!(payload.as_ref().unwrap()["source"], "s1");
        }
        DeltaRow::Delete { id } => panic!("expected upsert {expected_id}, got delete {id}"),
    }
}

fn assert_delete(row: &DeltaRow, expected_id: u64) {
    match row {
        DeltaRow::Delete { id } => assert_eq!(*id, expected_id),
        DeltaRow::Upsert { id, .. } => panic!("expected delete {expected_id}, got upsert {id}"),
    }
}

#[tokio::test]
#[ignore = "requires a live Iceberg REST catalog and MinIO"]
async fn scan_delta_tracks_live_append_and_data_file_drop() -> Result<(), Box<dyn Error>> {
    let config = config();
    let catalog = build_rest_catalog(&config).await?;
    let namespace = unique_namespace();
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    let table_ident = TableIdent::new(namespace.clone(), "vectors".to_string());
    let creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .schema(arrow_schema_to_schema(arrow_schema().as_ref())?)
        .build();
    let table = catalog.create_table(&namespace, creation).await?;

    let (table, s0_file) = append_rows(
        &catalog,
        &table,
        "s0",
        &[1, 2],
        &[[1.0, 10.0], [2.0, 20.0]],
        &[r#"{"source":"s0"}"#, r#"{"source":"s0"}"#],
    )
    .await?;
    let s0 = table
        .metadata()
        .current_snapshot()
        .expect("S0 exists")
        .snapshot_id();

    // A separately-built catalog and freshly-loaded table model an external writer.
    let external_catalog = build_rest_catalog(&config).await?;
    let external_table = external_catalog.load_table(&table_ident).await?;
    let (external_table, _) = append_rows(
        &external_catalog,
        &external_table,
        "s1",
        &[3, 4],
        &[[3.0, 30.0], [4.0, 40.0]],
        &[r#"{"source":"s1"}"#, r#"{"source":"s1"}"#],
    )
    .await?;
    let s2 = commit_file_drop(&config.catalog_uri, &external_table, &s0_file).await?;

    let source_table = catalog.load_table(&table_ident).await?;
    let source_binding = binding(&namespace, &table_ident);
    let delta = scan_delta(
        &source_table,
        SnapshotDelta {
            from_snapshot_id: Some(s0),
            to_snapshot_id: s2,
        },
        &source_binding,
    )
    .await?;
    assert_eq!(delta.unresolved_delete_files, 0);
    assert_eq!(delta.rows.len(), 4);
    assert_upsert(&delta.rows[0], 3, &[3.0, 30.0]);
    assert_upsert(&delta.rows[1], 4, &[4.0, 40.0]);
    assert_delete(&delta.rows[2], 1);
    assert_delete(&delta.rows[3], 2);

    let full = scan_delta(
        &source_table,
        SnapshotDelta {
            from_snapshot_id: None,
            to_snapshot_id: s2,
        },
        &source_binding,
    )
    .await?;
    assert_eq!(full.unresolved_delete_files, 0);
    assert_eq!(full.rows.len(), 2);
    let mut full_ids = full
        .rows
        .iter()
        .map(|row| match row {
            DeltaRow::Upsert { id, .. } => *id,
            DeltaRow::Delete { id } => panic!("full scan emitted delete {id}"),
        })
        .collect::<Vec<_>>();
    full_ids.sort_unstable();
    assert_eq!(full_ids, vec![3, 4]);

    let error = match scan_delta(
        &source_table,
        SnapshotDelta {
            from_snapshot_id: Some(i64::MAX),
            to_snapshot_id: s2,
        },
        &source_binding,
    )
    .await
    {
        Ok(_) => panic!("non-ancestor source snapshot must require a full rescan"),
        Err(error) => error,
    };
    assert!(
        matches!(error, LakehouseError::Schema(ref message) if message.contains("not an ancestor")),
        "unexpected non-ancestor error: {error}"
    );

    catalog.drop_table(&table_ident).await?;
    catalog.drop_namespace(&namespace).await?;
    Ok(())
}
