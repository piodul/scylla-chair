use std::sync::Arc;

use histogram::Histogram;
use parking_lot::{Mutex, MutexGuard};
use thread_local::ThreadLocal;

/// A sharded histogram.
///
/// This structure is optimize for frequent writes and seldom reads.
/// For each program thread, it keeps a separate instance of the Histogram
/// structure. When the histogram is needed for reading, all shards are combined
/// into a single histogram and returned.
///
/// Each shard is protected by a separate parking_lot::Mutex - assuming that
/// the structure is read unfrequently, they will be uncontended most of the time.
/// When getting a combined histogram instance, the reader has to get access to
/// all shards, locks them one at a time, and returns the combined result.
///
/// TODO: Maybe a single histogram under a mutex would perform better? Dunno,
/// less dereferencing...
pub struct ShardedHistogram {
    shards: ThreadLocal<Arc<Mutex<Histogram>>>,
    all: Mutex<Vec<Arc<Mutex<Histogram>>>>,
    config: histogram::Config,
}

impl ShardedHistogram {
    pub fn new(config: histogram::Config) -> Self {
        Self {
            shards: ThreadLocal::new(),
            all: Mutex::new(Vec::new()),
            config,
        }
    }

    pub fn get_shard_mut(&self) -> MutexGuard<'_, Histogram> {
        self.shards
            .get_or(|| {
                let shard = Arc::new(Mutex::new(self.config.build().unwrap()));
                self.all.lock().push(shard.clone());
                shard
            })
            .lock()
    }

    pub fn get_combined_and_clear(&self) -> Histogram {
        let mut hist = self.config.build().unwrap();
        for shard in self.all.lock().iter() {
            let shard = &mut shard.lock();
            hist.merge(shard);
            shard.clear();
        }
        hist
    }
}
