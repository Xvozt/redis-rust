use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
enum StoredData {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
    Stream(Vec<Entry>),
}

#[derive(Debug, Clone)]
struct Entry {
    id: EntryId,
    values: HashMap<String, Vec<u8>>,
}

impl Entry {
    fn new(values: HashMap<String, Vec<u8>>) -> Self {
        let id = EntryId::new();
        Self { id, values }
    }

    fn with_id(id_str: &str, values: HashMap<String, Vec<u8>>) -> Result<Self, String> {
        let id = id_str.parse::<EntryId>()?;
        Ok(Self { id, values })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct EntryId {
    ms: u64,
    seq: u64,
}

impl EntryId {
    fn new() -> Self {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Cannot create timestamp for EntryID")
            .as_millis() as u64;

        Self { ms, seq: 0 }
    }
}

impl FromStr for EntryId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split("-");

        let ms = parts
            .next()
            .ok_or("Missing first part: {ms}-{sequence}")?
            .parse::<u64>()
            .map_err(|_| "Invalid first id part")?;

        let seq = parts
            .next()
            .ok_or("Missing second part: {ms}-{sequence}")?
            .parse::<u64>()
            .map_err(|_| "Invalid sequence second id part")?;

        if parts.next().is_some() {
            return Err("Too many parts in ID".to_string());
        }

        Ok(EntryId { ms, seq })
    }
}

impl Ord for EntryId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ms.cmp(&other.ms).then(self.seq.cmp(&other.seq))
    }
}
impl PartialOrd for EntryId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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

struct Waiter {
    keys: Vec<String>,
    sender: Sender<(String, Vec<u8>)>,
}

#[derive(Clone)]
pub struct Storage {
    inner: Arc<Mutex<HashMap<String, StoredValue>>>,
    waiters: Arc<Mutex<VecDeque<Waiter>>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            waiters: Arc::new(Mutex::new(VecDeque::new())),
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
                        let len = list.len();
                        drop(store);
                        self.notify_waiters(&key);
                        return Ok(len);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
            }
        }

        let len = values.len();
        store.insert(key.clone(), StoredValue::new(StoredData::List(values)));
        drop(store);
        self.notify_waiters(&key);
        Ok(len)
    }

    pub fn lpush(&self, key: String, values: Vec<Vec<u8>>) -> Result<usize, String> {
        let mut store = self.inner.lock().unwrap();

        let mut result = values.into_iter().rev().collect::<Vec<Vec<u8>>>();

        if let Some(stored_value) = store.get_mut(&key) {
            if stored_value.is_expired() {
                store.remove(&key);
            } else {
                match &mut stored_value.data {
                    StoredData::List(list) => {
                        result.append(list);
                        *list = result;
                        let len = list.len();
                        drop(store);
                        self.notify_waiters(&key);
                        return Ok(len);
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
            }
        }

        let len = result.len();
        store.insert(key.clone(), StoredValue::new(StoredData::List(result)));
        drop(store);
        self.notify_waiters(&key);
        Ok(len)
    }

    pub fn lrange(&self, key: &str, start: isize, end: isize) -> Result<Vec<Vec<u8>>, String> {
        let mut store = self.inner.lock().unwrap();
        match store.get(key) {
            None => return Ok(vec![]),
            Some(stored_value) => {
                if stored_value.is_expired() {
                    store.remove(key);
                    return Ok(vec![]);
                }

                match &stored_value.data {
                    StoredData::List(list) => {
                        let len = list.len() as isize;

                        let start_idx = if start < 0 {
                            (len + start).max(0) as usize
                        } else {
                            start as usize
                        };

                        let end_idx = if end < 0 {
                            (len + end).max(0) as usize
                        } else if end >= len {
                            list.len() - 1
                        } else {
                            end as usize
                        };

                        if start_idx > end_idx || start_idx >= list.len() {
                            return Ok(vec![]);
                        }

                        Ok(list[start_idx..=end_idx].to_vec())
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
            }
        }
    }

    pub fn llen(&self, key: &str) -> Result<usize, String> {
        let mut store = self.inner.lock().unwrap();

        match store.get(key) {
            None => Ok(0),
            Some(stored_value) => {
                if stored_value.is_expired() {
                    store.remove(key);
                    return Ok(0);
                }

                match &stored_value.data {
                    StoredData::List(list) => return Ok(list.len()),
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
            }
        }
    }

    pub fn lpop(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let mut store = self.inner.lock().unwrap();
        match store.get_mut(key) {
            None => Ok(None),
            Some(stored_value) => {
                if stored_value.is_expired() {
                    store.remove(key);
                    return Ok(None);
                }

                match &mut stored_value.data {
                    StoredData::List(list) => {
                        if list.is_empty() {
                            store.remove(key);
                            return Ok(None);
                        }
                        let element = list.remove(0);
                        if list.is_empty() {
                            store.remove(key);
                        }
                        return Ok(Some(element));
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
            }
        }
    }

    pub fn lpop_multiple(
        &self,
        key: &str,
        mut count: usize,
    ) -> Result<Option<Vec<Vec<u8>>>, String> {
        let mut store = self.inner.lock().unwrap();
        match store.get_mut(key) {
            None => Ok(None),
            Some(stored_value) => {
                if stored_value.is_expired() {
                    store.remove(key);
                    return Ok(None);
                }

                match &mut stored_value.data {
                    StoredData::List(list) => {
                        if list.is_empty() {
                            store.remove(key);
                            return Ok(None);
                        }
                        if count > list.len() {
                            count = list.len();
                        }
                        let elements = list.drain(0..count).collect();
                        if list.is_empty() {
                            store.remove(key);
                        }
                        return Ok(Some(elements));
                    }
                    _ => {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        )
                    }
                }
            }
        }
    }

    pub fn blpop(
        &self,
        keys: Vec<String>,
        timeout_secs: f64,
    ) -> Result<Option<(String, Vec<u8>)>, String> {
        for key in &keys {
            let store = self.inner.lock().unwrap();

            if let Some(stored_value) = store.get(key) {
                if !matches!(stored_value.data, StoredData::List(_)) {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
            }

            drop(store);

            if let Some(value) = self.lpop(key)? {
                return Ok(Some((key.clone(), value)));
            }
        }

        let (tx, rx) = mpsc::channel();

        let waiter = Waiter {
            keys: keys.clone(),
            sender: tx,
        };

        self.waiters.lock().unwrap().push_back(waiter);

        let result = if timeout_secs == 0.0 {
            match rx.recv() {
                Ok((key, value)) => Some((key, value)),
                Err(_) => unreachable!("Channel shouldn't be closed"),
            }
        } else {
            let timeout = Duration::from_secs_f64(timeout_secs);
            match rx.recv_timeout(timeout) {
                Ok((key, value)) => Some((key, value)),
                Err(_) => {
                    self.waiters.lock().unwrap().retain(|w| w.keys != keys);
                    None
                }
            }
        };
        Ok(result)
    }

    pub fn exists(&self, key: &str) -> bool {
        let store = self.inner.lock().unwrap();
        store.contains_key(key)
    }
    pub fn delete(&self, key: &str) -> bool {
        let mut store = self.inner.lock().unwrap();
        store.remove(key).is_some()
    }

    fn notify_waiters(&self, key: &str) {
        let waiter: Option<Waiter> = {
            let mut waiters = self.waiters.lock().unwrap();
            waiters
                .iter()
                .position(|w| w.keys.contains(&key.to_string()))
                .and_then(|pos| waiters.remove(pos))
        };

        if let Some(waiter) = waiter {
            if let Ok(Some(value)) = self.lpop(key) {
                let _ = waiter.sender.send((key.to_string(), value));
            }
        }
    }

    pub fn get_type(&self, key: &str) -> String {
        let store = self.inner.lock().unwrap();

        match store.get(key) {
            None => "none".to_string(),
            Some(stored_value) => {
                if stored_value.is_expired() {
                    "none".to_string()
                } else {
                    match stored_value.data {
                        StoredData::List(_) => "list".to_string(),
                        StoredData::String(_) => "string".to_string(),
                        StoredData::Stream(_) => "stream".to_string(),
                    }
                }
            }
        }
    }

    pub fn xadd(
        &self,
        key: String,
        id: &str,
        values: HashMap<String, Vec<u8>>,
    ) -> Result<String, String> {
        todo!()
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

    #[test]
    fn test_lpush_list_not_exist() {
        let storage = Storage::new();
        let result = storage.lpush("my_list".to_string(), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(result, Ok(2));

        let list = storage.lrange("my_list", 0, -1);
        assert_eq!(list, Ok(vec![b"b".to_vec(), b"a".to_vec()]))
    }

    #[test]
    fn test_lpush_list_exist() {
        let storage = Storage::new();
        storage
            .lpush("my_list".to_string(), vec![b"c".to_vec()])
            .unwrap();
        // list is: [c]

        let result = storage.lpush("my_list".to_string(), vec![b"b".to_vec(), b"a".to_vec()]);
        // b pushed first -> [b, c]
        // a pushed second -> [a, b, c]
        assert_eq!(result, Ok(3));

        let list = storage.lrange("my_list", 0, -1);
        assert_eq!(list, Ok(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]))
    }

    #[test]
    fn test_lpush_wrong_type() {
        let storage = Storage::new();
        storage.set("key".to_string(), b"value".to_vec());
        let err = storage.lpush(
            "key".to_string(),
            vec![b"first".to_vec(), b"second".to_vec()],
        );

        assert_eq!(
            err,
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        )
    }

    #[test]
    fn test_lrange_works() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"first".to_vec(), b"second".to_vec()],
        );
        let result = storage.lrange("my_list", 0, 0);
        assert_eq!(result, Ok(vec![b"first".to_vec()]))
    }

    #[test]
    fn test_lrange_works_multiple_values() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()],
        );
        let result = storage.lrange("my_list", 0, 2);
        assert_eq!(
            result,
            Ok(vec![
                b"first".to_vec(),
                b"second".to_vec(),
                b"third".to_vec()
            ])
        )
    }

    #[test]
    fn test_lrange_with_negative_indexes() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![
                b"a".to_vec(),
                b"a".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
            ],
        );
        let result = storage.lrange("my_list", -2, -1);
        assert_eq!(result, Ok(vec![b"d".to_vec(), b"e".to_vec()]))
    }

    #[test]
    fn test_lrange_with_mix_start_is_positive_end_is_negative() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );
        let result = storage.lrange("my_list", 0, -1);
        assert_eq!(
            result,
            Ok(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        )
    }

    #[test]
    fn test_lrange_with_mix_start_is_negative_end_is_positive() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );
        let result = storage.lrange("my_list", -2, 2);
        assert_eq!(result, Ok(vec![b"b".to_vec(), b"c".to_vec()]))
    }

    #[test]
    fn test_lrange_with_negative_indexes_only_last_element() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );
        let result = storage.lrange("my_list", -1, -1);
        assert_eq!(result, Ok(vec![b"c".to_vec()]))
    }

    #[test]
    fn test_lrange_with_negative_indexes_start_is_bigger_than_end() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );
        let result = storage.lrange("my_list", -1, -3);
        assert_eq!(result, Ok(vec![]))
    }

    #[test]
    fn test_lrange_with_negative_indexes_both_out_of_range() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );
        let result = storage.lrange("my_list", -100, -200);
        assert_eq!(result, Ok(vec![b"a".to_vec()]))
    }

    #[test]
    fn test_lrange_returns_empty_array_if_list_doesnt_exist() {
        let storage = Storage::new();
        let result = storage.lrange("my_list", 0, 1);
        assert_eq!(result, Ok(vec![]))
    }

    #[test]
    fn test_lrange_returns_empty_array_if_start_pos_is_bigger_than_stop_pos() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"first".to_vec(), b"second".to_vec()],
        );
        let result = storage.lrange("my_list", 5, 0);
        assert_eq!(result, Ok(vec![]))
    }

    #[test]
    fn test_lrange_returns_empty_array_if_start_pos_is_bigger_than_list_len() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"first".to_vec(), b"second".to_vec()],
        );
        let result = storage.lrange("my_list", 2, 5);
        assert_eq!(result, Ok(vec![]))
    }

    #[test]
    fn test_lrange_stop_pos_is_last_if_stop_is_bigger_than_list_len() {
        let storage = Storage::new();
        let _list = storage.rpush(
            "my_list".to_string(),
            vec![b"first".to_vec(), b"second".to_vec()],
        );
        let result = storage.lrange("my_list", 0, 5);
        assert_eq!(result, Ok(vec![b"first".to_vec(), b"second".to_vec()]))
    }

    #[test]
    fn test_lrange_doest_work_for_maps() {
        let storage = Storage::new();
        storage.set("key".to_string(), b"value".to_vec());
        let err = storage.lrange("key", 0, 0);

        assert_eq!(
            err,
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        )
    }

    #[test]
    fn test_llen_works_for_existing_list() {
        let storage = Storage::new();
        let _list = storage.rpush("my_list".to_string(), vec![b"a".to_vec(), b"b".to_vec()]);
        let result = storage.llen("my_list");
        assert_eq!(result, Ok(2))
    }

    #[test]
    fn test_llen_works_for_non_existing_list_returns_zero_len() {
        let storage = Storage::new();
        let result = storage.llen("my_list");
        assert_eq!(result, Ok(0))
    }

    #[test]
    fn test_llen_doest_work_for_maps() {
        let storage = Storage::new();
        storage.set("key".to_string(), b"value".to_vec());
        let err = storage.llen("key");

        assert_eq!(
            err,
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        )
    }

    #[test]
    fn test_lpop_works_for_existing_list() {
        let storage = Storage::new();

        let _list = storage.rpush(
            "list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );

        let poped_element = storage.lpop("list");
        assert_eq!(poped_element, Ok(Some(b"a".to_vec())));

        let left_elements = storage.lrange("list", 0, -1);

        assert_eq!(left_elements, Ok(vec![b"b".to_vec(), b"c".to_vec()]));
    }

    #[test]
    fn test_lpop_returns_null_string_for_nonexisting_list() {
        let storage = Storage::new();

        let poped_element = storage.lpop("nonexisting_list");
        assert_eq!(poped_element, Ok(None));
    }

    #[test]
    fn test_lpop_returns_null_string_for_empty_list() {
        let storage = Storage::new();

        let _list = storage.rpush("list".to_string(), vec![b"a".to_vec()]);

        let poped_element = storage.lpop("list");
        assert_eq!(poped_element, Ok(Some(b"a".to_vec())));

        let left_elements = storage.lrange("list", 0, -1);
        assert_eq!(left_elements, Ok(vec![]));

        let poped_element = storage.lpop("list");
        assert_eq!(poped_element, Ok(None));
    }

    #[test]
    fn test_lpop_doest_work_for_maps() {
        let storage = Storage::new();
        storage.set("key".to_string(), b"value".to_vec());
        let err = storage.lpop("key");

        assert_eq!(
            err,
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        )
    }

    #[test]
    fn test_lpop_multiple_works_with_number_to_return() {
        let storage = Storage::new();

        let _list = storage.rpush(
            "list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );

        let poped_element = storage.lpop_multiple("list", 2);
        assert_eq!(poped_element, Ok(Some(vec![b"a".to_vec(), b"b".to_vec()])));

        let left_elements = storage.lrange("list", 0, -1);

        assert_eq!(left_elements, Ok(vec![b"c".to_vec()]));
    }

    #[test]
    fn test_lpop_multiple_works_with_number_to_delete_and_returns_array_even_for_1_popped_element()
    {
        let storage = Storage::new();

        let _list = storage.rpush(
            "list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );

        let poped_element = storage.lpop_multiple("list", 1);
        assert_eq!(poped_element, Ok(Some(vec![b"a".to_vec()])));

        let left_elements = storage.lrange("list", 0, -1);

        assert_eq!(left_elements, Ok(vec![b"b".to_vec(), b"c".to_vec()]));
    }

    #[test]
    fn test_lpop_multiple_works_with_zero_count_and_returns_empty_array() {
        let storage = Storage::new();

        let _list = storage.rpush(
            "list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );

        let poped_element = storage.lpop_multiple("list", 0);
        assert_eq!(poped_element, Ok(Some(vec![])));

        let left_elements = storage.lrange("list", 0, -1);

        assert_eq!(
            left_elements,
            Ok(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
    }

    #[test]
    fn test_lpop_multiple_removes_and_returns_whole_list_if_number_is_greater_than_list_len() {
        let storage = Storage::new();

        let _list = storage.rpush(
            "list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );

        let poped_element = storage.lpop_multiple("list", 4);
        assert_eq!(
            poped_element,
            Ok(Some(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]))
        );

        let left_elements = storage.lrange("list", 0, -1);

        assert_eq!(left_elements, Ok(vec![]));
    }

    #[test]
    fn test_lpop_multiple_returns_none_for_non_existing_key() {
        let storage = Storage::new();

        let result = storage.lpop_multiple("not_exist", 1);
        assert_eq!(result, Ok(None))
    }

    #[test]
    fn test_lpop_multiple_doest_work_for_maps() {
        let storage = Storage::new();
        storage.set("key".to_string(), b"value".to_vec());
        let err = storage.lpop_multiple("key", 1);

        assert_eq!(
            err,
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        )
    }

    #[test]
    fn test_blpop_command_works_and_returns_result_immediately_if_at_least_one_key_is_present() {
        let storage = Storage::new();

        let _list = storage.rpush(
            "list".to_string(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        );

        let popped = storage.blpop(
            vec!["list".to_string(), "list2".to_string(), "list3".to_string()],
            10.0,
        );
        assert_eq!(Ok(Some(("list".to_string(), b"a".to_vec()))), popped);
    }

    #[test]
    fn test_blpop_comand_with_timeout_works_and_returns_array_with_key_and_popped_value() {
        let storage = Storage::new();

        let storage_clone = storage.clone();

        let handle = std::thread::spawn(move || {
            let result = storage_clone.blpop(vec!["list".to_string()], 0.1);
            result
        });

        std::thread::sleep(Duration::from_millis(50));

        storage
            .rpush("list".to_string(), vec![b"a".to_vec()])
            .unwrap();

        let result = handle.join().unwrap();
        assert_eq!(result, Ok(Some(("list".to_string(), b"a".to_vec()))));
    }

    #[test]
    fn test_blpop_comand_fifo_ordering_first_waiter_gets_value() {
        let storage = Storage::new();

        let storage_first = storage.clone();

        let client_one =
            std::thread::spawn(move || storage_first.blpop(vec!["list".to_string()], 10.0));

        std::thread::sleep(Duration::from_millis(100));

        let storage_two = storage.clone();
        let _client_two =
            std::thread::spawn(move || storage_two.blpop(vec!["list".to_string()], 10.0));

        std::thread::sleep(Duration::from_millis(100));

        storage
            .rpush("list".to_string(), vec![b"a".to_vec()])
            .unwrap();

        let result_one = client_one.join().unwrap();
        assert_eq!(result_one, Ok(Some(("list".to_string(), b"a".to_vec()))));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(storage.lrange("list", 0, -1), Ok(vec![]));
    }

    #[test]
    fn test_blpop_comand_with_timeout_zero_works_infinitely() {
        let storage = Storage::new();

        let storage_clone = storage.clone();

        let handle = std::thread::spawn(move || {
            let result = storage_clone.blpop(vec!["infinite".to_string()], 0.0);
            result
        });

        std::thread::sleep(Duration::from_millis(100));

        storage
            .rpush("infinite".to_string(), vec![b"a".to_vec()])
            .unwrap();

        let result = handle.join().unwrap();
        assert_eq!(result, Ok(Some(("infinite".to_string(), b"a".to_vec()))));
    }

    #[test]
    fn test_blpop_command_doesnt_work_on_maps() {
        let storage = Storage::new();
        storage.set("key".to_string(), b"value".to_vec());
        let err = storage.blpop(vec![String::from("key")], 10.0);

        assert_eq!(
            err,
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        )
    }

    #[test]
    fn test_get_type_command_returns_some() {
        let storage = Storage::new();

        let result = storage.rpush("my_list".to_string(), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(result, Ok(2));

        let t = storage.get_type("my_list");
        assert_eq!(t, "list".to_string());
    }

    #[test]
    fn test_get_type_command_returns_none() {
        let storage = Storage::new();

        let t = storage.get_type("my_list");
        assert_eq!(t, "none".to_string());
    }

    #[test]
    #[ignore = "xadd command not implemented yet"]
    fn test_xadd_create_stream_with_passed_id() {
        let storage = Storage::new();
        let mut values = HashMap::new();
        values.insert("key".to_string(), b"value".to_vec());

        let result = storage.xadd("mystream".to_string(), "0-1", values);

        assert_eq!(result, Ok("0-1".to_string()));
        assert!(storage.exists("mystream"));
        assert_eq!(storage.get_type("mystream"), "stream");
    }

    #[test]
    #[ignore = "xadd command not implemented yet"]
    fn test_xadd_create_stream_with_generated_id() {
        let storage = Storage::new();
        let mut values = HashMap::new();
        values.insert("key".to_string(), b"value".to_vec());

        let result = storage.xadd("mystream".to_string(), "*", values);

        assert!(result.is_ok());
        assert!(result.unwrap().contains('-'));
    }
}
