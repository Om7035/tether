use crate::error::TetherError;
use crate::Result;
use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

/// The file's first 8 bytes hold the true data length (little-endian u64) so
/// that pre-growth padding (see GROWTH_CHUNK) can never be mistaken for real
/// records: the file itself may be padded past its real data, but this
/// header always says exactly how much of it is real.
const HEADER_LEN: u64 = 8;

/// Grow the file by at least this much whenever the mmap needs to expand, so
/// most writes land inside the existing mapping instead of remapping every time.
const GROWTH_CHUNK: u64 = 64 * 1024;

/// Append-only WAL. Writes accumulate in an in-memory buffer; `sync` flushes
/// the buffer into a persistently held mmap'd file. Record format (after the
/// 8-byte length header): [u32 len][bytes], back to back.
pub struct WriteAheadLog {
    path: PathBuf,
    file: File,
    mmap: Option<MmapMut>,
    /// Bytes of real record data written so far, not counting the header.
    data_len: u64,
    buffer: Vec<u8>,
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
            .truncate(false)
            .open(&path)
            .map_err(TetherError::Io)?;
        let data_len = read_header(&path)?;
        Ok(Self {
            path,
            file,
            mmap: None,
            data_len,
            buffer: Vec::new(),
        })
    }

    /// Appends an entry to the in-memory buffer. Returns the offset (past
    /// the header) it will occupy once flushed.
    pub fn append(&mut self, entry: &[u8]) -> Result<u64> {
        let offset = self.data_len + self.buffer.len() as u64;
        self.buffer.extend_from_slice(&(entry.len() as u32).to_le_bytes());
        self.buffer.extend_from_slice(entry);
        Ok(offset)
    }

    /// Flushes the in-memory buffer into the mmap and clears it. Reuses the
    /// held file handle and mmap across calls; only grows/remaps the file
    /// when the current mapping can't fit the new data. Updates the header
    /// last, after the record bytes are durably in the mapping, so a torn
    /// flush never advances the header past data that didn't make it.
    pub fn sync(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let new_data_len = self.data_len + self.buffer.len() as u64;
        let new_file_len = HEADER_LEN + new_data_len;
        let mapped_len = self.mmap.as_ref().map(|m| m.len() as u64).unwrap_or(0);

        if new_file_len > mapped_len {
            let grown_len = new_file_len.max(mapped_len + GROWTH_CHUNK);
            self.file.set_len(grown_len).map_err(TetherError::Io)?;
            self.mmap = Some(unsafe {
                MmapMut::map_mut(&self.file).map_err(TetherError::Io)?
            });
        }

        let mmap = self.mmap.as_mut().expect("mmap initialized above");
        let start = (HEADER_LEN + self.data_len) as usize;
        mmap[start..start + self.buffer.len()].copy_from_slice(&self.buffer);
        mmap.flush_range(start, self.buffer.len())
            .map_err(TetherError::Io)?;

        mmap[0..8].copy_from_slice(&new_data_len.to_le_bytes());
        mmap.flush_range(0, 8).map_err(TetherError::Io)?;

        self.data_len = new_data_len;
        self.buffer.clear();
        Ok(())
    }

    /// Reads every committed (flushed) entry back from disk, in order.
    pub fn replay(&self) -> Result<Vec<Vec<u8>>> {
        replay_file(&self.path, self.data_len)
    }

    /// Truncates the log back to empty. Only supports truncating to 0 for
    /// now (drop the whole log after a full checkpoint).
    pub fn truncate(&mut self, up_to: u64) -> Result<()> {
        if up_to != 0 {
            return Err(TetherError::InvalidEntry(
                "truncate only supports up_to=0".into(),
            ));
        }
        self.mmap = None;
        self.file.set_len(0).map_err(TetherError::Io)?;
        self.data_len = 0;
        self.buffer.clear();
        Ok(())
    }
}

/// Reads the 8-byte data-length header from a WAL file. A file shorter than
/// the header (new or empty) has zero data.
fn read_header(path: &Path) -> Result<u64> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(TetherError::Io(e)),
    };
    if (bytes.len() as u64) < HEADER_LEN {
        return Ok(0);
    }
    Ok(u64::from_le_bytes(bytes[0..8].try_into().unwrap()))
}

fn replay_file(path: &Path, data_len: u64) -> Result<Vec<Vec<u8>>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(TetherError::Io(e)),
    };
    if (bytes.len() as u64) < HEADER_LEN {
        return Ok(Vec::new());
    }
    let start = HEADER_LEN as usize;
    let end = (start as u64 + data_len).min(bytes.len() as u64) as usize;
    let bytes = &bytes[start..end];

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

    #[test]
    fn many_syncs_grow_past_the_initial_chunk_and_still_replay_correctly() {
        // Regression test for the persistent-mmap redesign: writes that
        // outgrow the pre-grown mapping must trigger a remap, not corrupt
        // or truncate already-written data, and padding must never be
        // mistaken for real records.
        let path = temp_wal_path("grow");
        let _ = std::fs::remove_file(&path);
        let mut wal = WriteAheadLog::new(&path).unwrap();

        let big_entry = vec![b'x'; 10_000];
        let mut expected = Vec::new();
        for _ in 0..20 {
            wal.append(&big_entry).unwrap();
            wal.sync().unwrap();
            expected.push(big_entry.clone());
        }

        let recovered = wal.replay().unwrap();
        assert_eq!(recovered, expected);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reopening_after_growth_padding_does_not_see_phantom_records() {
        // Regression test for the header-length fix: without it, a fresh
        // handle reopening a pre-grown file would read the raw (padded)
        // file size as data length, and zero-byte padding parses as valid
        // (but bogus) zero-length WRITE records.
        let path = temp_wal_path("reopen_after_growth");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = WriteAheadLog::new(&path).unwrap();
            wal.append(b"only-entry").unwrap();
            wal.sync().unwrap();
        }

        let wal2 = WriteAheadLog::new(&path).unwrap();
        let recovered = wal2.replay().unwrap();
        assert_eq!(recovered, vec![b"only-entry".to_vec()]);

        let _ = std::fs::remove_file(&path);
    }
}
