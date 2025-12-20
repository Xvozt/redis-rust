use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug)]
struct StoredValue {
    data: Vec<u8>,
    expired_at: Option<SystemTime>,
}

impl StoredValue {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            expired_at: None,
        }
    }

    fn with_expiration(data: Vec<u8>, expires_at: SystemTime) -> Self {
        Self {
            data,
            expired_at: Some(expires_at),
        }
    }

    fn is_expired(&self) -> bool {
        match self.expired_at {
            Some(expire) => SystemTime::now() >= expire,
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct Storage {
    inner: Arc<Mutex<HashMap<String, StoredValue>>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>) {
        let mut store = self.inner.lock().unwrap();
        store.insert(key, StoredValue::new(value));
    }

    pub fn set_ex(&self, key: String, value: Vec<u8>, seconds: u64) {
        let expires_at = SystemTime::now() + Duration::from_secs(seconds);
        let mut store = self.inner.lock().unwrap();
        store.insert(key, StoredValue::with_expiration(value, expires_at));
    }

    pub fn set_px(&self, key: String, value: Vec<u8>, milliseconds: u64) {
        let expires_at = SystemTime::now() + Duration::from_millis(milliseconds);
        let mut store = self.inner.lock().unwrap();
        store.insert(key, StoredValue::with_expiration(value, expires_at));
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut store = self.inner.lock().unwrap();
        if let Some(stored_value) = store.get(key) {
            if stored_value.is_expired() {
                store.remove(key);
                return None;
            }
            return Some(stored_value.data.clone());
        }
        None
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
    use std::thread::sleep;

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

    #[test]
    fn test_get_expired_returns_none() {
        let storage = Storage::new();
        storage.set_ex("key".to_string(), b"value".to_vec(), 1);
        sleep(Duration::from_millis(1100));
        assert_eq!(storage.get("key"), None);
    }

    #[test]
    fn test_get_non_expired() {
        let storage = Storage::new();
        storage.set_ex("key".to_string(), b"value".to_vec(), 100);
        assert_eq!(storage.get("key"), Some(b"value".to_vec()));
    }
}
