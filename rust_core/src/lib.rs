use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Arc;
use parking_lot::Mutex;

mod wal;
mod state;
mod error;

pub use wal::WriteAheadLog;
pub use state::StateManager;
pub use error::TetherError;

pub type Result<T> = std::result::Result<T, TetherError>;

/// Encodes a key and value into a simple length-prefixed format.
/// Format: [u32 key_len][key bytes][value bytes]
fn encode_entry(key: &str, value: &[u8]) -> Vec<u8> {
    let key_bytes = key.as_bytes();
    let key_len = key_bytes.len() as u32;
    let mut encoded = Vec::with_capacity(4 + key_bytes.len() + value.len());
    encoded.extend_from_slice(&key_len.to_le_bytes());
    encoded.extend_from_slice(key_bytes);
    encoded.extend_from_slice(value);
    encoded
}

/// Decodes a length-prefixed entry back into key and value.
/// Returns (key, value) or an error if the data is malformed.
#[allow(dead_code)]
fn decode_entry(data: &[u8]) -> Result<(String, Vec<u8>)> {
    if data.len() < 4 {
        return Err(TetherError::InvalidEntry(
            "entry too short to contain key_len".into(),
        ));
    }
    let key_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let start = 4;
    let end = start + key_len;
    if end > data.len() {
        return Err(TetherError::InvalidEntry(
            "entry truncated: key_len extends past data".into(),
        ));
    }
    let key = String::from_utf8(data[start..end].to_vec()).map_err(|e| {
        TetherError::InvalidEntry(format!("key is not valid UTF-8: {}", e))
    })?;
    let value = data[end..].to_vec();
    Ok((key, value))
}

#[pyclass]
pub struct TetherEngine {
    wal: Arc<Mutex<WriteAheadLog>>,
    state: Arc<StateManager>,
}

#[pymethods]
impl TetherEngine {
    #[new]
    pub fn new(wal_dir: &str) -> PyResult<Self> {
        let wal = WriteAheadLog::new(wal_dir).map_err(|e| -> PyErr { e.into() })?;
        let state = StateManager::new();
        Ok(Self {
            wal: Arc::new(Mutex::new(wal)),
            state: Arc::new(state),
        })
    }

    /// Writes a key-value pair to state (marks pending) and appends to WAL.
    /// The WAL append and sync operations release the GIL via py.allow_threads.
    pub fn write(
        &mut self,
        py: Python,
        key: &str,
        value: &[u8],
    ) -> PyResult<()> {
        // Mark the key as pending in state (no GIL release needed for DashMap).
        self.state
            .mark_pending(key.to_string(), value.to_vec())
            .map_err(|e| -> PyErr { e.into() })?;

        // Encode the key-value pair and append to WAL, with GIL released.
        let encoded = encode_entry(key, value);
        let wal = Arc::clone(&self.wal);
        py.allow_threads(|| {
            let mut wal_guard = wal.lock();
            wal_guard.append(&encoded)?;
            wal_guard.sync()?;
            Ok::<(), TetherError>(())
        })
        .map_err(|e| -> PyErr { e.into() })?;

        Ok(())
    }

    /// Commits a pending key in state. Also releases GIL for consistency.
    pub fn commit(&mut self, py: Python, key: &str) -> PyResult<()> {
        let state = Arc::clone(&self.state);
        py.allow_threads(|| {
            state.commit(key).map_err(|e| -> PyErr { e.into() })
        })
    }

    /// Reads a committed value from state. Returns PyBytes for zero-copy semantics.
    /// Returns None if the key is not found or is still pending.
    pub fn read(&self, py: Python, key: &str) -> PyResult<Option<Py<PyBytes>>> {
        if let Some(bytes) = self.state.get(key) {
            Ok(Some(PyBytes::new_bound(py, &bytes).unbind().into()))
        } else {
            Ok(None)
        }
    }
}

#[pymodule]
fn tether(_py: Python, m: &pyo3::Bound<'_, pyo3::types::PyModule>) -> PyResult<()> {
    m.add_class::<TetherEngine>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let key = "test_key";
        let value = b"test_value";
        let encoded = encode_entry(key, value);
        let (decoded_key, decoded_value) = decode_entry(&encoded).unwrap();
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_value, value);
    }

    #[test]
    fn test_decode_truncated_entry() {
        let data = vec![0x05, 0x00, 0x00, 0x00]; // key_len = 5, but no key data
        let result = decode_entry(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_non_utf8_key() {
        let mut data = vec![0x02, 0x00, 0x00, 0x00]; // key_len = 2
        data.push(0xFF);
        data.push(0xFE); // Invalid UTF-8 bytes
        let result = decode_entry(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_then_commit_then_read() {
        let temp_dir = std::env::temp_dir();
        let wal_path = temp_dir
            .join(format!("tether_test_write_commit_{}", std::process::id()))
            .to_string_lossy()
            .into_owned();

        // Create engine
        let engine = TetherEngine::new(&wal_path).unwrap();

        // Write a key-value pair (marks pending and writes to WAL)
        // Note: this is a unit test so we can't use py.allow_threads, call directly
        engine.state.mark_pending("test_key".to_string(), b"test_value".to_vec())
            .unwrap();
        let encoded = encode_entry("test_key", b"test_value");
        {
            let mut wal = engine.wal.lock();
            wal.append(&encoded).unwrap();
            wal.sync().unwrap();
        }

        // At this point, the key is pending, so read should return None
        assert_eq!(engine.state.get("test_key"), None);

        // Commit the key
        engine.state.commit("test_key").unwrap();

        // Now read should return the value
        assert_eq!(engine.state.get("test_key"), Some(b"test_value".to_vec()));

        // Verify WAL can be replayed
        {
            let wal = engine.wal.lock();
            let entries = wal.replay().unwrap();
            assert_eq!(entries.len(), 1);
            let (decoded_key, decoded_value) = decode_entry(&entries[0]).unwrap();
            assert_eq!(decoded_key, "test_key");
            assert_eq!(decoded_value, b"test_value");
        }

        let _ = std::fs::remove_file(&wal_path);
    }
}
