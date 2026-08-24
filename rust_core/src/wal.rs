use crate::error::TetherError;
use crate::Result;
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

/// Append-only WAL. Writes accumulate in an in-memory buffer; `sync` flushes
/// the buffer to an mmap'd file. Record format: [u32 len][bytes], back to back.
pub struct WriteAheadLog {
    path: PathBuf,
    buffer: Vec<u8>,
    file_len: u64,
}

impl WriteAheadLog {
    pub fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(TetherError::Io)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(TetherError::Io)?;
        let file_len = file.metadata().map_err(TetherError::Io)?.len();
        Ok(Self {
            path,
            buffer: Vec::new(),
            file_len,
        })
    }

    /// Appends an entry to the in-memory buffer. Returns the offset it will
    /// occupy in the file once flushed.
    pub fn append(&mut self, entry: &[u8]) -> Result<u64> {
        let offset = self.file_len + self.buffer.len() as u64;
        self.buffer.extend_from_slice(&(entry.len() as u32).to_le_bytes());
        self.buffer.extend_from_slice(entry);
        Ok(offset)
    }

    /// Flushes the in-memory buffer to the mmap'd file and clears it.
    pub fn sync(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let new_len = self.file_len + self.buffer.len() as u64;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(TetherError::Io)?;
        file.set_len(new_len).map_err(TetherError::Io)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file).map_err(TetherError::Io)? };
        let start = self.file_len as usize;
        mmap[start..start + self.buffer.len()].copy_from_slice(&self.buffer);
        mmap.flush().map_err(TetherError::Io)?;

        self.file_len = new_len;
        self.buffer.clear();
        Ok(())
    }

    /// Reads every committed (flushed) entry back from disk, in order.
    pub fn replay(&self) -> Result<Vec<Vec<u8>>> {
        replay_file(&self.path)
    }

    /// Truncates the file to zero length below `up_to` bytes. Only supports
    /// truncating to 0 for now (drop the whole log after a full checkpoint).
    pub fn truncate(&mut self, up_to: u64) -> Result<()> {
        if up_to != 0 {
            return Err(TetherError::InvalidEntry(
                "truncate only supports up_to=0".into(),
            ));
        }
        let file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(TetherError::Io)?;
        file.set_len(0).map_err(TetherError::Io)?;
        self.file_len = 0;
        self.buffer.clear();
        Ok(())
    }
}

fn replay_file(path: &Path) -> Result<Vec<Vec<u8>>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(TetherError::Io(e)),
    };
    let mut entries = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > bytes.len() {
            break; // truncated/torn write, stop replay here
        }
        entries.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_wal_path(name: &str) -> String {
        let mut dir = std::env::temp_dir();
        dir.push(format!("tether_wal_test_{}_{}", name, std::process::id()));
        dir.to_string_lossy().into_owned()
    }

    #[test]
    fn append_buffers_in_memory_without_touching_disk() {
        let path = temp_wal_path("append_mem");
        let _ = std::fs::remove_file(&path);
        let mut wal = WriteAheadLog::new(&path).unwrap();

        wal.append(b"hello").unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn sync_flushes_buffer_to_mmap_file_and_replay_recovers_entries() {
        let path = temp_wal_path("flush_replay");
        let _ = std::fs::remove_file(&path);
        let mut wal = WriteAheadLog::new(&path).unwrap();

        wal.append(b"first").unwrap();
        wal.append(b"second").unwrap();
        wal.sync().unwrap();

        assert!(std::fs::metadata(&path).unwrap().len() > 0);

        let recovered = wal.replay().unwrap();
        assert_eq!(recovered, vec![b"first".to_vec(), b"second".to_vec()]);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn replay_survives_reopening_from_a_fresh_handle() {
        let path = temp_wal_path("reopen");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = WriteAheadLog::new(&path).unwrap();
            wal.append(b"durable").unwrap();
            wal.sync().unwrap();
        }
        // Simulate process restart: brand new WriteAheadLog handle over same file.
        let wal2 = WriteAheadLog::new(&path).unwrap();
        let recovered = wal2.replay().unwrap();
        assert_eq!(recovered, vec![b"durable".to_vec()]);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn truncate_clears_the_log() {
        let path = temp_wal_path("truncate");
        let _ = std::fs::remove_file(&path);
        let mut wal = WriteAheadLog::new(&path).unwrap();
        wal.append(b"gone").unwrap();
        wal.sync().unwrap();

        wal.truncate(0).unwrap();
        assert_eq!(wal.replay().unwrap(), Vec::<Vec<u8>>::new());

        let _ = std::fs::remove_file(&path);
    }
}
