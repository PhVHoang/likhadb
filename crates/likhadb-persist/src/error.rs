use std::io;

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("encode error: {0}")]
    Encode(#[source] bincode::Error),
    #[error("decode error: {0}")]
    Decode(#[source] bincode::Error),
    #[error("WAL CRC mismatch at mid-log frame: expected {expected:#010x}, got {got:#010x}")]
    Crc { expected: u32, got: u32 },
    #[error("WAL replay error: {0}")]
    Apply(#[from] likhadb_core::LikhaDbError),
}
