use pyo3::prelude::*;
use std::sync::Arc;
use dashmap::DashMap;

mod wal;
mod state;
mod error;

pub use wal::WriteAheadLog;
pub use state::StateManager;
pub use error::TetherError;

pub type Result<T> = std::result::Result<T, TetherError>;

#[pyclass]
pub struct TetherEngine {
    wal: Arc<WriteAheadLog>,
    state: Arc<StateManager>,
}

#[pymethods]
impl TetherEngine {
    #[new]
    pub fn new(wal_dir: &str) -> PyResult<Self> {
        todo!()
    }

    pub fn write(&mut self, key: &str, value: &[u8]) -> PyResult<()> {
        todo!()
    }

    pub fn read(&self, key: &str) -> PyResult<Option<Vec<u8>>> {
        todo!()
    }

    pub fn commit(&mut self) -> PyResult<()> {
        todo!()
    }
}

#[pymodule]
fn tether(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<TetherEngine>()?;
    Ok(())
}
