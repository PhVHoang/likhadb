use std::io;

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("encode error: {0}")]
    Encode(#[source] bincode::Error),
    #[error("decode error: {0}")]
    Decode(#[source] bincode::Error),
    #[error("unsupported WAL version {found}; maximum supported version is {max}")]
    UnsupportedVersion { found: u8, max: u8 },
    #[error("WAL checksum mismatch at mid-log frame: expected {expected:#018x}, got {got:#018x}")]
    Crc { expected: u64, got: u64 },
    #[error("WAL replay error: {0}")]
    Apply(#[from] likhadb_core::LikhaDbError),
}
