use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug)]
enum StoredData {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
}

#[derive(Clone, Debug)]
struct StoredValue {
    data: StoredData,
    expired_at: Option<SystemTime>,
}

impl StoredValue {
    fn new(data: StoredData) -> Self {
        Self {
            data,
            expired_at: None,
        }
    }

    fn with_expiration(data: StoredData, expires_at: SystemTime) -> Self {
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

    fn as_string(&self) -> Option<&Vec<u8>> {
        match &self.data {
            StoredData::String(bytes) => Some(bytes),
            _ => None,
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
        store.insert(key, StoredValue::new(StoredData::String(value)));
    }

    pub fn set_ex(&self, key: String, value: Vec<u8>, seconds: u64) {
        let expires_at = SystemTime::now() + Duration::from_secs(seconds);
        let mut store = self.inner.lock().unwrap();
        store.insert(
            key,
            StoredValue::with_expiration(StoredData::String(value), expires_at),
        );
    }

    pub fn set_px(&self, key: String, value: Vec<u8>, milliseconds: u64) {
        let expires_at = SystemTime::now() + Duration::from_millis(milliseconds);
        let mut store = self.inner.lock().unwrap();
        store.insert(
            key,
            StoredValue::with_expiration(StoredData::String(value), expires_at),
        );
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut store = self.inner.lock().unwrap();
        if let Some(stored_value) = store.get(key) {
            if stored_value.is_expired() {
                store.remove(key);
                return None;
            }
            return stored_value.as_string().cloned();
        }
        None
    }

    pub fn rpush(&self, key: String, values: Vec<Vec<u8>>) -> Result<usize, String> {
        let mut store = self.inner.lock().unwrap();

        if let Some(stored_value) = store.get_mut(&key) {
            if stored_value.is_expired() {
                store.remove(&key);
            } else {
                match &mut stored_value.data {
                    StoredData::List(list) => {
                        list.extend(values);
                        return Ok(list.len());
                    }
                    StoredData::String(_) => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
            }
        }

        let len = values.len();
        store.insert(key, StoredValue::new(StoredData::List(values)));
        Ok(len)
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

    #[test]
    fn test_rpush_list_not_exist() {
        let storage = Storage::new();
        let result = storage.rpush(
            "my_list".to_string(),
            vec![b"first".to_vec(), b"second".to_vec()],
        );
        assert_eq!(result, Ok(2))
    }

    #[test]
    fn test_rpush_list_exist() {
        let storage = Storage::new();
        storage
            .rpush("my_list".to_string(), vec![b"first".to_vec()])
            .unwrap();

        let result = storage.rpush(
            "my_list".to_string(),
            vec![b"second".to_vec(), b"third".to_vec()],
        );
        assert_eq!(result, Ok(3))
    }

    #[test]
    fn test_rpush_wrong_type() {
        let storage = Storage::new();
        storage.set("key".to_string(), b"value".to_vec());
        let err = storage.rpush(
            "key".to_string(),
            vec![b"first".to_vec(), b"second".to_vec()],
        );

        assert_eq!(
            err,
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        )
    }
}
