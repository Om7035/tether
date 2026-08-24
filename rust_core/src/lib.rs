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

const RECORD_WRITE: u8 = 0;
const RECORD_COMMIT: u8 = 1;

/// Encodes a key and value into a length-prefixed WRITE record.
/// Format: [u8 record_type][u32 key_len][key bytes][value bytes]
fn encode_entry(key: &str, value: &[u8]) -> Vec<u8> {
    let key_bytes = key.as_bytes();
    let key_len = key_bytes.len() as u32;
    let mut encoded = Vec::with_capacity(1 + 4 + key_bytes.len() + value.len());
    encoded.push(RECORD_WRITE);
    encoded.extend_from_slice(&key_len.to_le_bytes());
    encoded.extend_from_slice(key_bytes);
    encoded.extend_from_slice(value);
    encoded
}

/// Encodes a COMMIT record for a key: the WAL's record of phase two of 2PC.
/// Format: [u8 record_type][u32 key_len][key bytes]
fn encode_commit(key: &str) -> Vec<u8> {
    let key_bytes = key.as_bytes();
    let key_len = key_bytes.len() as u32;
    let mut encoded = Vec::with_capacity(1 + 4 + key_bytes.len());
    encoded.push(RECORD_COMMIT);
    encoded.extend_from_slice(&key_len.to_le_bytes());
    encoded.extend_from_slice(key_bytes);
    encoded
}

/// A decoded WAL record: either a pending write or a commit marker.
enum WalRecord {
    Write { key: String, value: Vec<u8> },
    Commit { key: String },
}

/// Decodes a single WAL record (WRITE or COMMIT).
fn decode_entry(data: &[u8]) -> Result<WalRecord> {
    if data.is_empty() {
        return Err(TetherError::InvalidEntry("empty entry".into()));
    }
    let record_type = data[0];
    if data.len() < 5 {
        return Err(TetherError::InvalidEntry(
            "entry too short to contain key_len".into(),
        ));
    }
    let key_len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
    let start = 5;
    let end = start + key_len;
    if end > data.len() {
        return Err(TetherError::InvalidEntry(
            "entry truncated: key_len extends past data".into(),
        ));
    }
    let key = String::from_utf8(data[start..end].to_vec()).map_err(|e| {
        TetherError::InvalidEntry(format!("key is not valid UTF-8: {}", e))
    })?;
    match record_type {
        RECORD_WRITE => Ok(WalRecord::Write {
            key,
            value: data[end..].to_vec(),
        }),
        RECORD_COMMIT => Ok(WalRecord::Commit { key }),
        other => Err(TetherError::InvalidEntry(format!(
            "unknown WAL record type: {}",
            other
        ))),
    }
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

        // Replay the WAL to reconstruct state from a prior run. A key is only
        // restored as Committed if its WRITE record is followed by a matching
        // COMMIT record — a write with no commit means the process died before
        // phase two, so it must be re-executed, not treated as done.
        let entries = wal.replay().map_err(|e| -> PyErr { e.into() })?;
        let mut pending_writes: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::new();
        for entry in entries {
            match decode_entry(&entry) {
                Ok(WalRecord::Write { key, value }) => {
                    pending_writes.insert(key, value);
                }
                Ok(WalRecord::Commit { key }) => {
                    if let Some(value) = pending_writes.remove(&key) {
                        state.set(key, value).map_err(|e| -> PyErr { e.into() })?;
                    }
                }
                Err(_) => continue, // skip malformed/torn records
            }
        }

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

    /// Commits a pending key: appends a COMMIT record to the WAL (so replay
    /// can tell this write completed phase two) and marks it Committed in
    /// state. Both happen inside py.allow_threads since the WAL append syncs
    /// to disk.
    pub fn commit(&mut self, py: Python, key: &str) -> PyResult<()> {
        let state = Arc::clone(&self.state);
        let wal = Arc::clone(&self.wal);
        let encoded = encode_commit(key);
        py.allow_threads(|| {
            {
                let mut wal_guard = wal.lock();
                wal_guard.append(&encoded)?;
                wal_guard.sync()?;
            }
            state.commit(key)
        })
        .map_err(|e| -> PyErr { e.into() })
    }

    /// Reads a committed value from state. Returns PyBytes for zero-copy semantics.
    /// Returns None if the key is not found or is still pending.
    pub fn read(&self, py: Python, key: &str) -> PyResult<Option<Py<PyBytes>>> {
        if let Some(bytes) = self.state.get(key) {
            Ok(Some(PyBytes::new_bound(py, &bytes).unbind()))
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
    fn test_encode_decode_write_roundtrip() {
        let key = "test_key";
        let value = b"test_value";
        let encoded = encode_entry(key, value);
        match decode_entry(&encoded).unwrap() {
            WalRecord::Write { key: k, value: v } => {
                assert_eq!(k, key);
                assert_eq!(v, value);
            }
            WalRecord::Commit { .. } => panic!("expected Write record"),
        }
    }

    #[test]
    fn test_encode_decode_commit_roundtrip() {
        let key = "test_key";
        let encoded = encode_commit(key);
        match decode_entry(&encoded).unwrap() {
            WalRecord::Commit { key: k } => assert_eq!(k, key),
            WalRecord::Write { .. } => panic!("expected Commit record"),
        }
    }

    #[test]
    fn test_decode_truncated_entry() {
        let data = vec![RECORD_WRITE, 0x05, 0x00, 0x00, 0x00]; // key_len = 5, but no key data
        let result = decode_entry(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_non_utf8_key() {
        let mut data = vec![RECORD_WRITE, 0x02, 0x00, 0x00, 0x00]; // key_len = 2
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
            match decode_entry(&entries[0]).unwrap() {
                WalRecord::Write { key, value } => {
                    assert_eq!(key, "test_key");
                    assert_eq!(value, b"test_value");
                }
                WalRecord::Commit { .. } => panic!("expected a Write record"),
            }
        }

        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn restart_does_not_treat_an_uncommitted_write_as_committed() {
        // Regression test: a process killed between write() and commit()
        // leaves a WRITE record with no matching COMMIT record in the WAL.
        // On restart, that key must NOT be visible via read() -- it was
        // never durably committed, so the step must be re-executed.
        let temp_dir = std::env::temp_dir();
        let wal_path = temp_dir
            .join(format!("tether_test_uncommitted_{}", std::process::id()))
            .to_string_lossy()
            .into_owned();
        let _ = std::fs::remove_file(&wal_path);

        {
            let engine = TetherEngine::new(&wal_path).unwrap();
            engine
                .state
                .mark_pending("never_committed".to_string(), b"orphaned".to_vec())
                .unwrap();
            let encoded = encode_entry("never_committed", b"orphaned");
            let mut wal = engine.wal.lock();
            wal.append(&encoded).unwrap();
            wal.sync().unwrap();
            // Process "dies" here -- commit() is never called.
        }

        // Simulate restart: fresh engine replays the same WAL.
        let engine2 = TetherEngine::new(&wal_path).unwrap();
        assert_eq!(
            engine2.state.get("never_committed"),
            None,
            "an uncommitted write must not be visible after replay"
        );

        let _ = std::fs::remove_file(&wal_path);
    }
}
