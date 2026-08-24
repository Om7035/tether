use dashmap::DashMap;
use crate::Result;
use std::sync::Arc;

pub struct StateManager {
    store: Arc<DashMap<String, Vec<u8>>>,
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        todo!()
    }

    pub fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        todo!()
    }

    pub fn delete(&self, key: &str) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub fn iter(&self) -> Result<Vec<(String, Vec<u8>)>> {
        todo!()
    }
}

impl Default for StateManager {
    fn default() -> Self {
        Self::new()
    }
}
