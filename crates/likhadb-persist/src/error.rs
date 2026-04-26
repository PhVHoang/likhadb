use std::io;

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("encode error: {0}")]
    Encode(#[source] bincode::Error),
    #[error("decode error: {0}")]
    Decode(#[source] bincode::Error),
}
