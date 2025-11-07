use crate::buffers::errors::CapacityError;
use crate::buffers::pow2_ceil;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::{cmp, mem};

pub const MIN_BUCKET: usize = 256;
pub const MAX_BUCKET: usize = 1 << 20; // 1 MiB

pub struct SmartBufferPool {
    entry: Arc<SmartEntry>,
}

pub struct SmartEntry {
    buckets: Vec<Mutex<Vec<Vec<u8>>>>,
    bytes_in_use: AtomicUsize,
    #[allow(dead_code)]
    max_bytes: usize,
    hit_count: AtomicUsize,
    miss_count: AtomicUsize,
}

impl Default for SmartBufferPool {
    fn default() -> Self {
        Self::new(8 * 1024 * 1024) // 8 MiB default max
    }
}

impl SmartBufferPool {
    pub fn new(max_bytes: usize) -> Self {
        let mut caps = Vec::new();
        let mut c = MIN_BUCKET;
        while c <= MAX_BUCKET {
            caps.push(c);
            c <<= 1;
        }

        let buckets = caps.into_iter().map(|_| Mutex::new(Vec::new())).collect();
        let entry = Arc::new(SmartEntry {
            bytes_in_use: AtomicUsize::new(0),
            buckets,
            max_bytes,
            hit_count: AtomicUsize::new(0),
            miss_count: AtomicUsize::new(0),
        });
        Self { entry }
    }

    pub fn get(&self, min_capacity: usize) -> SmartPage {
        if self.bytes_in_pool() > self.entry.max_bytes {
            self.trim();
        }

        // let want = pow2_ceil(min_capacity.max(MIN_BUCKET)).min(MAX_BUCKET);
        let want = pow2_ceil(min_capacity).max(MIN_BUCKET);
        if want <= MAX_BUCKET {
            let index = self.bucket_index(want);
            if let Ok(mut bin) = self.entry.buckets[index].lock()
                && let Some(mut buf) = bin.pop()
            {
                self.entry.hit_count.fetch_add(1, Ordering::Relaxed);
                buf.clear();
                return SmartPage {
                    buf,
                    cap_bucket: want,
                    pool: Arc::downgrade(&self.entry),
                };
            }
        }

        self.entry.miss_count.fetch_add(1, Ordering::Relaxed);
        let buf = Vec::with_capacity(want);
        self.entry.bytes_in_use.fetch_add(want, Ordering::Relaxed);
        SmartPage {
            buf,
            cap_bucket: want,
            pool: Arc::downgrade(&self.entry),
        }
    }

    #[inline(always)]
    pub(crate) fn bucket_index(&self, cap: usize) -> usize {
        // This optimized version assumes `cap` is already a power of two,
        // which the `get` method guarantees.
        const MIN_BUCKET_LOG2: u32 = MIN_BUCKET.trailing_zeros();
        const MAX_BUCKET_LOG2: u32 = MAX_BUCKET.trailing_zeros();
        const MAX_INDEX: usize = (MAX_BUCKET_LOG2 - MIN_BUCKET_LOG2) as usize;

        // Calculate the log2 of the capacity.
        let cap_log2 = cap.trailing_zeros();

        // Calculate the index relative to the minimum bucket size.
        // .saturating_sub ensures that if cap is somehow smaller than MIN_BUCKET,
        // it returns 0 instead of panicking.
        let index = (cap_log2.saturating_sub(MIN_BUCKET_LOG2)) as usize;

        // Clamp the index to the maximum valid index. This is the crucial
        // step that handles requests larger than MAX_BUCKET.
        index.min(MAX_INDEX)
    }

    pub fn bytes_in_pool(&self) -> usize {
        self.entry.bytes_in_use.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> (usize, usize) {
        (
            self.entry.hit_count.load(Ordering::Relaxed),
            self.entry.miss_count.load(Ordering::Relaxed),
        )
    }

    pub fn trim(&self) {
        for bin in self.entry.buckets.iter() {
            let mut bin = bin.lock().unwrap();
            for buf in bin.drain(..) {
                self.entry
                    .bytes_in_use
                    .fetch_sub(buf.capacity(), Ordering::Relaxed);
            }
        }
    }
}

impl Clone for SmartBufferPool {
    fn clone(&self) -> Self {
        Self {
            entry: Arc::clone(&self.entry),
        }
    }
}

pub struct SmartPage {
    pub(crate) buf: Vec<u8>,
    cap_bucket: usize,
    pool: Weak<SmartEntry>,
}

impl SmartPage {
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    #[inline(always)]
    pub fn vec_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    #[inline(always)]
    pub fn append_slice(&mut self, data: &[u8]) -> Result<(), CapacityError> {
        let new_len = self
            .buf
            .len()
            .checked_add(data.len())
            .ok_or(CapacityError)?;

        // Check if the new length exceeds the current capacity
        if new_len > self.buf.capacity() {
            // Return an error if capacity is insufficient
            return Err(CapacityError);
        }

        // If capacity is sufficient, safely extend the vector
        // Note: We use `extend_from_slice` which is safe here because we've checked
        // the required space. It won't reallocate (panic) because we know
        // `new_len <= capacity`.
        self.buf.extend_from_slice(data);
        Ok(())
    }

    /// Resize, extending with uninitialized space.
    pub fn resize_uninit(&mut self, new_len: usize) {
        if new_len > self.buf.len() {
            let additional = new_len - self.buf.len();
            self.buf.reserve(additional);
            // Initialize the spare capacity and then set the length.
            unsafe {
                let spare = self.buf.spare_capacity_mut();
                let to_init = cmp::min(additional, spare.len());
                for slot in &mut spare[..to_init] {
                    *slot = mem::MaybeUninit::uninit();
                }
                // Now it's safe to update the vector length to include the new uninitialized bytes.
                self.buf.set_len(new_len);
            }
        } else {
            self.buf.truncate(new_len);
        }
    }
}

impl AsRef<[u8]> for SmartPage {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for SmartPage {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let cap = self.buf.capacity();
            // Skip extremely large buffers (don’t cache).
            if self.cap_bucket > MAX_BUCKET {
                pool.bytes_in_use.fetch_sub(cap, Ordering::Relaxed);
                return;
            }

            // let idx = pool.bucket_index(cap);
            let idx = {
                const MIN_BUCKET_LOG2: u32 = MIN_BUCKET.trailing_zeros();
                const MAX_BUCKET_LOG2: u32 = MAX_BUCKET.trailing_zeros();
                const MAX_INDEX: usize = (MAX_BUCKET_LOG2 - MIN_BUCKET_LOG2) as usize;

                let cap_log2 = self.cap_bucket.trailing_zeros();
                let index = (cap_log2.saturating_sub(MIN_BUCKET_LOG2)) as usize;
                index.min(MAX_INDEX)
            };
            self.buf.clear();

            if let Ok(mut bin) = pool.buckets[idx].lock() {
                bin.push(mem::take(&mut self.buf));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Barrier, thread, usize::MAX};

    use super::*;
    fn total_buffers(pool: &SmartBufferPool) -> usize {
        pool.entry
            .buckets
            .iter()
            .map(|b| b.lock().unwrap().len())
            .sum()
    }

    #[test]
    fn test_basic_allocation_resue() {
        let pool = SmartBufferPool::new(1 << 20); // 1MB
        let cap = 1024;

        let b1 = pool.get(cap);
        assert!(b1.capacity() >= cap);
        drop(b1);
        assert_eq!(total_buffers(&pool), 1);

        let b2 = pool.get(cap);
        assert_eq!(b2.capacity(), pow2_ceil(cap).min(MAX_BUCKET));
        let (hits, miss) = pool.stats();
        assert!(hits >= 1, "should register a hit");
        assert!(miss >= 1, "should register a miss on first alloc");
        drop(b2);

        assert_eq!(total_buffers(&pool), 1);
    }

    #[test]
    fn test_different_bucket_sizes() {
        let pool = SmartBufferPool::new(8 << 20);
        let small = pool.get(300); // should round to 512
        let med = pool.get(2000); // ~2048
        let large = pool.get(10000); // ~16384

        assert_eq!(small.capacity(), 512);
        assert_eq!(med.capacity(), 2048);
        assert_eq!(large.capacity(), 16384);

        drop(small);
        drop(med);
        drop(large);
        assert!(total_buffers(&pool) >= 3);
    }

    #[test]
    fn test_byte_tracking_and_trim() {
        let pool = SmartBufferPool::new(8 << 20);
        let before = pool.bytes_in_pool();
        {
            let _b1 = pool.get(4096);
            let _b2 = pool.get(8192);
        }

        let after = pool.bytes_in_pool();
        assert!(after >= before);

        pool.trim();
        let trimmed = pool.bytes_in_pool();
        assert!(trimmed <= before);
    }

    #[test]
    fn test_large_buffer_not_cached() {
        let pool = SmartBufferPool::new(8 << 20);
        let big = pool.get(MAX_BUCKET * 2);
        let cap = big.capacity();
        assert!(cap > MAX_BUCKET);
        drop(big);

        // should not be cached due to > MAX_BUCKET
        assert_eq!(total_buffers(&pool), 0);
    }

    #[test]
    fn test_concurrent_allocation() {
        let pool = Arc::new(SmartBufferPool::new(16 << 20));
        let threads = 16;
        let iterations = 200;
        let barrier = Arc::new(Barrier::new(threads));

        let mut handles = Vec::new();
        for _ in 0..threads {
            let pool_clone = pool.clone();
            let barrier_clone = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier_clone.wait();
                for i in 0..iterations {
                    let size = 256 * ((i % 8) + 1);
                    let mut buf = pool_clone.get(size);
                    assert!(buf.capacity() >= size);
                    buf.as_mut_slice();
                    drop(buf);
                }
            }))
        }

        for h in handles {
            h.join().unwrap();
        }

        let (hits, misses) = pool.stats();
        assert!(hits > 0);
        assert!(misses > 0);
        assert!(pool.bytes_in_pool() <= pool.entry.max_bytes * 2);
    }

    #[test]
    fn test_high_pressure_trim_behavior() {
        let pool = SmartBufferPool::new(4 << 20);
        let mut bufs = Vec::new();

        for _ in 1..100 {
            bufs.push(pool.get(1024));
        }
        drop(bufs);

        assert!(pool.bytes_in_pool() <= pool.entry.max_bytes * 2);
        pool.trim();
        assert!(pool.bytes_in_pool() <= pool.entry.max_bytes);
    }

    #[test]
    fn test_reuse_patterns_multiple_sizes() {
        let pool = SmartBufferPool::new(16 << 20);
        for _ in 0..10 {
            let mut a = pool.get(512);
            let mut b = pool.get(4096);
            a.resize_uninit(512);
            b.resize_uninit(4096);
            drop(a);
            drop(b);
        }
        let total = total_buffers(&pool);
        assert!(total >= 2);

        let (hits, misses) = pool.stats();
        assert!(hits > 0);
        assert!(misses > 0);
    }

    #[test]
    fn test_resize_uninit_and_clear() {
        let pool = SmartBufferPool::new(8 << 20);
        let mut buf = pool.get(512);
        buf.resize_uninit(1024);
        assert_eq!(buf.len(), 1024);
        buf.clear();
        assert_eq!(buf.len(), 0);
    }

    /*************  ✨ Windsurf Command ⭐  *************/
    /// Test that the pool can handle repeated get/drop patterns.
    ///
    /// This test case is important because it checks that the pool can
    /// handle the case where a thread repeatedly gets and drops buffers
    /// without ever blocking to wait for another thread to return a
    /// buffer. This is a common case in many applications, and it is
    /// important that the pool can handle this case efficiently.
    /*******  eac7cb69-5bd8-4b06-9dd1-e76372848914  *******/
    #[test]
    fn test_repeated_get_drop_patterns() {
        let pool = SmartBufferPool::new(8 << 20);
        for _ in 0..1000 {
            let mut buf = pool.get(512);
            buf.resize_uninit(4096);
            drop(buf);
        }

        let (hits, misses) = pool.stats();
        assert!(hits > 0);
        assert!(misses > 0);
        assert!(pool.bytes_in_pool() <= pool.entry.max_bytes);
    }

    #[test]
    fn test_pressure_behaviour_over_limit() {
        let pool = SmartBufferPool::new(4 << 20);
        let mut allocated = Vec::new();
        for _ in 0..128 {
            allocated.push(pool.get(65536));
        }
        assert!(pool.bytes_in_pool() <= pool.entry.max_bytes * 2);
    }

    #[test]
    fn test_trim_after_large_spike() {
        let pool = SmartBufferPool::new(16 << 20);
        let mut bufs = Vec::new();
        for _ in 0..100 {
            bufs.push(pool.get(32768));
        }
        drop(bufs);

        let before_trim = pool.bytes_in_pool();
        pool.trim();
        let after_trim = pool.bytes_in_pool();
        assert!(after_trim <= before_trim);

        assert!(total_buffers(&pool) <= total_buffers(&pool));
    }

    #[test]
    fn test_auto_return_behaviour_drop() {
        let pool = SmartBufferPool::new(8 << 20);
        {
            let b = pool.get(1024);
            assert_eq!(total_buffers(&pool), 0);
            drop(b);
        }
        assert_eq!(total_buffers(&pool), 1);
    }

    #[test]
    fn test_large_scale_random_sizes() {
        let pool = SmartBufferPool::new(128 << 20);
        let mut rng_state = 12345u64;
        fn next_u64(state: &mut u64) -> u64 {
            *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *state
        }

        for _ in 0..10_000 {
            let rand_val = (next_u64(&mut rng_state) >> 16) as usize;
            let size = (rand_val % (MAX_BUCKET * 2)).max(1);
            let buf = pool.get(size);
            assert!(buf.capacity() >= size.min(MAX_BUCKET));
            drop(buf);
        }

        let (hits, misses) = pool.stats();
        assert!(hits > 0);
        assert!(misses > 0);
    }

    #[test]
    fn test_stability_under_multiple_threads_long_run() {
        let pool = Arc::new(SmartBufferPool::new(64 << 20));
        let threads = 8;
        let iterations = 2000;

        let mut handles = Vec::new();
        for _ in 0..threads {
            let pool_clone = pool.clone();
            handles.push(thread::spawn(move || {
                for i in 0..iterations {
                    let size = ((i * 37) % (MAX_BUCKET / 4)) + 128;
                    let mut buf = pool_clone.get(size);
                    buf.resize_uninit(size);
                    buf.as_mut_slice()[0] = 42;
                    drop(buf);
                }
            }))
        }

        for h in handles {
            h.join().unwrap();
        }

        let (hits, misses) = pool.stats();
        assert!(hits > 0);
        assert!(misses > 0);
        assert!(pool.bytes_in_pool() <= pool.entry.max_bytes * 2);
    }
}
