use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TetherError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid WAL entry: {0}")]
    InvalidEntry(String),

    #[error("State error: {0}")]
    StateError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

impl From<TetherError> for PyErr {
    fn from(err: TetherError) -> Self {
        PyRuntimeError::new_err(err.to_string())
    }
}
