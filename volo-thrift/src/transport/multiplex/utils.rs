use std::array;

use tokio::sync::Mutex;

const SHARD_COUNT: i32 = 64;

pub struct TxHashMap<T> {
    sharded: [Mutex<fxhash::FxHashMap<i32, T>>; SHARD_COUNT as usize],
}

impl<T> Default for TxHashMap<T> {
    fn default() -> Self {
        TxHashMap {
            sharded: array::from_fn(|_| Default::default()),
        }
    }
}

impl<T> TxHashMap<T>
where
    T: Sized,
{
    fn get_shard(&self, key: i32) -> &Mutex<fxhash::FxHashMap<i32, T>> {
        &self.sharded[((key % SHARD_COUNT + SHARD_COUNT) % SHARD_COUNT) as usize]
    }
    pub async fn remove(&self, key: &i32) -> Option<T> {
        self.get_shard(*key).lock().await.remove(key)
    }

    pub async fn is_empty(&self) -> bool {
        for s in self.sharded.iter() {
            if !s.lock().await.is_empty() {
                return false;
            }
        }
        true
    }

    pub async fn insert(&self, key: i32, value: T) -> Option<T> {
        self.get_shard(key).lock().await.insert(key, value)
    }

    pub async fn for_all_drain(&self, mut f: impl FnMut(T)) {
        for sharded in self.sharded.iter() {
            let mut s = sharded.lock().await;
            for data in s.drain() {
                f(data.1)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::TxHashMap;

    #[tokio::test]
    async fn test_txmap() {
        let mp = TxHashMap::default();
        mp.insert(-1, 1).await;
        assert_eq!(mp.remove(&-1).await.unwrap(), 1);

        mp.insert(1<<31, 1).await;
        assert_eq!(mp.remove(&(1<<31)).await.unwrap(), 1);
    }
}
