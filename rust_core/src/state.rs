use dashmap::DashMap;
use crate::Result;
use std::sync::Arc;

/// Tracks the commit state of a value in the 2-Phase Commit protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryState {
    /// Value is pending (marked but not yet committed).
    Pending,
    /// Value is committed (durably written and visible).
    Committed,
}

/// A state manager implementing 2-Phase Commit (2PC) semantics.
/// Values are stored as (state, data) tuples. Before execution, state is
/// marked Pending. After successful execution, it is marked Committed.
/// Only Committed values are visible to `get()`.
pub struct StateManager {
    /// Maps key -> (state, value). Pending entries are invisible to get().
    store: Arc<DashMap<String, (EntryState, Vec<u8>)>>,
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
        }
    }

    /// Returns the value ONLY if it is Committed.
    /// Pending values are not visible.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.store.get(key).and_then(|entry| {
            let (state, value) = entry.value();
            if *state == EntryState::Committed {
                Some(value.clone())
            } else {
                None
            }
        })
    }

    /// Marks a value as Pending. This makes it invisible to get() until committed.
    /// If the key already exists, it is overwritten.
    pub fn mark_pending(&self, key: String, value: Vec<u8>) -> Result<()> {
        self.store.insert(key, (EntryState::Pending, value));
        Ok(())
    }

    /// Transitions a Pending entry to Committed.
    /// Returns error if no Pending entry exists for this key.
    pub fn commit(&self, key: &str) -> Result<()> {
        let mut entry = self.store.get_mut(key)
            .ok_or_else(|| crate::error::TetherError::StateError(
                format!("No pending entry for key: {}", key)
            ))?;

        let (state, _) = entry.value_mut();
        if *state != EntryState::Pending {
            return Err(crate::error::TetherError::StateError(
                format!("Entry for key {} is not pending", key)
            ));
        }

        *state = EntryState::Committed;
        Ok(())
    }

    /// Returns true if a key has a Pending entry, false if Committed or missing.
    pub fn is_pending(&self, key: &str) -> bool {
        self.store.get(key)
            .map(|entry| entry.value().0 == EntryState::Pending)
            .unwrap_or(false)
    }

    /// Deletes a key (both pending and committed entries).
    /// Returns the value if it existed, None otherwise.
    pub fn delete(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.store.remove(key).map(|(_, (_, value))| value))
    }

    /// Iterates over all Committed entries (excludes Pending).
    /// Returns a Vec of (key, value) tuples.
    pub fn iter(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let mut result = Vec::new();
        for entry in self.store.iter() {
            let (key, (state, value)) = entry.pair();
            if *state == EntryState::Committed {
                result.push((key.clone(), value.clone()));
            }
        }
        Ok(result)
    }

    /// Legacy `set` method for compatibility. Marks a value as Committed immediately.
    pub fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        self.store.insert(key, (EntryState::Committed, value));
        Ok(())
    }
}

impl Default for StateManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_pending_then_get_returns_none() {
        let sm = StateManager::new();
        sm.mark_pending("key1".into(), b"value1".to_vec()).unwrap();
        assert_eq!(sm.get("key1"), None, "Pending values should not be visible via get()");
    }

    #[test]
    fn mark_pending_then_commit_then_get_returns_value() {
        let sm = StateManager::new();
        sm.mark_pending("key2".into(), b"value2".to_vec()).unwrap();
        sm.commit("key2").unwrap();
        assert_eq!(sm.get("key2"), Some(b"value2".to_vec()),
                   "Committed value should be visible via get()");
    }

    #[test]
    fn commit_on_nonexistent_key_returns_err() {
        let sm = StateManager::new();
        let result = sm.commit("nonexistent");
        assert!(result.is_err(), "Commit on nonexistent key should error");
        // Verify it doesn't panic or corrupt state
        assert_eq!(sm.get("nonexistent"), None);
    }

    #[test]
    fn is_pending_true_after_mark_pending_false_after_commit() {
        let sm = StateManager::new();
        sm.mark_pending("key3".into(), b"value3".to_vec()).unwrap();
        assert!(sm.is_pending("key3"), "is_pending should return true after mark_pending");

        sm.commit("key3").unwrap();
        assert!(!sm.is_pending("key3"), "is_pending should return false after commit");
    }

    #[test]
    fn second_mark_pending_on_committed_key_resets_to_pending() {
        let sm = StateManager::new();
        sm.mark_pending("key4".into(), b"value1".to_vec()).unwrap();
        sm.commit("key4").unwrap();
        assert_eq!(sm.get("key4"), Some(b"value1".to_vec()));
        assert!(!sm.is_pending("key4"));

        // New write cycle: mark as pending again with new value
        sm.mark_pending("key4".into(), b"value2".to_vec()).unwrap();
        assert!(sm.is_pending("key4"), "Re-marked key should be pending");
        assert_eq!(sm.get("key4"), None, "Pending re-mark should hide the committed value");

        sm.commit("key4").unwrap();
        assert_eq!(sm.get("key4"), Some(b"value2".to_vec()), "New value should be visible after commit");
    }
}
