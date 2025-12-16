use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Storage {
    inner: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>) {
        let mut store = self.inner.lock().unwrap();
        store.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let store = self.inner.lock().unwrap();
        store.get(key).cloned()
    }

    pub fn exists(&self, key: &str) -> bool {
        let store = self.inner.lock().unwrap();
        store.contains_key(key)
    }
    pub fn delete(&self, key: &str) -> bool {
        let mut store = self.inner.lock().unwrap();
        store.remove(key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get_success() {
        let storage = Storage::new();
        storage.set("key".to_string(), b"value".to_vec());
        assert_eq!(storage.get("key"), Some(b"value".to_vec()));
    }

    #[test]
    fn test_get_non_exist() {
        let storage = Storage::new();
        assert_eq!(storage.get("key-nonexistent"), None);
    }

    #[test]
    fn test_delete() {
        let storage = Storage::new();
        storage.set("key".to_string(), b"value".to_vec());
        assert!(storage.delete("key"));
        assert_eq!(storage.get("key"), None);
    }
}
