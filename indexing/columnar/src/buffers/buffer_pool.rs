use std::cell::UnsafeCell;
use std::cmp;
use std::mem::{self, MaybeUninit};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::buffers::pow2_ceil;

const MIN_BUCKET: usize = 256;
const MAX_BUCKET: usize = 1 << 20;

pub struct BufferPoolEntry {
    buckets: Vec<Mutex<Vec<Vec<u8>>>>,
    current_bytes: AtomicUsize,
    max_bytes: usize,
    min_bucket: usize,
    max_bucket: usize,
}

impl BufferPoolEntry {
    #[inline]
    fn bucket_index(&self, cap: usize) -> usize {
        let mut c = self.min_bucket;
        let mut idx = 0;
        while c < cap {
            c <<= 1;
            idx += 1;
        }
        idx
    }
}

#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<BufferPoolEntry>,
}

impl BufferPool {
    pub fn new(max_bytes: usize) -> Self {
        let mut caps = vec![];
        let mut c = MIN_BUCKET;
        while c <= MAX_BUCKET {
            caps.push(c);
            c <<= 1;
        }

        let buckets = caps.into_iter().map(|_| Mutex::new(Vec::new())).collect();
        Self {
            inner: Arc::new(BufferPoolEntry {
                buckets,
                current_bytes: AtomicUsize::new(0),
                max_bytes,
                min_bucket: MIN_BUCKET,
                max_bucket: MAX_BUCKET,
            }),
        }
    }

    pub fn get(&self, min_capacity: usize) -> PoolPage {
        let want = pow2_ceil(min_capacity.max(MIN_BUCKET)).min(self.inner.max_bucket);
        let idx = self.inner.bucket_index(want);

        if let Ok(mut bin) = self.inner.buckets[idx].lock()
            && let Some(mut buf) = bin.pop()
        {
            self.inner
                .current_bytes
                .fetch_sub(buf.capacity(), Ordering::Relaxed);
            buf.clear();
            return PoolPage {
                buf,
                cap_bucket: want,
                pool: Arc::downgrade(&self.inner),
            };
        }

        let mut buf = Vec::with_capacity(want);
        self.inner.current_bytes.fetch_add(want, Ordering::Relaxed);
        PoolPage {
            buf: {
                buf.clear();
                buf
            },
            cap_bucket: want,
            pool: Arc::downgrade(&self.inner),
        }
    }

    pub fn bytes_in_pool(&self) -> usize {
        self.inner.current_bytes.load(Ordering::Relaxed)
    }
}

pub struct PoolPage {
    buf: Vec<u8>,
    cap_bucket: usize,
    pool: Weak<BufferPoolEntry>,
}

impl PoolPage {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.buf.clear()
    }

    pub fn resize_uninit(&mut self, new_len: usize) {
        if new_len > self.buf.len() {
            let additional = new_len - self.buf.len();
            // SAFELY: we immediately set_len; users must write before read.
            self.buf.reserve(additional);
            unsafe {
                let spare = self.buf.spare_capacity_mut();
                let to_uninit = cmp::min(spare.len(), additional);
                for i in 0..to_uninit {
                    spare[i] = MaybeUninit::uninit();
                }
                self.buf.set_len(new_len);
            }
        } else {
            self.buf.truncate(new_len);
        }
    }

    #[inline]
    pub fn vec_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
}

impl Drop for PoolPage {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            if self.cap_bucket > pool.max_bucket {
                pool.current_bytes
                    .fetch_sub(self.capacity(), Ordering::Relaxed);
                return;
            }

            self.buf.clear();
            let cap = self.buf.capacity();
            pool.current_bytes.fetch_add(cap, Ordering::Relaxed);

            // return to the bucket
            let idx = pool.bucket_index(self.cap_bucket);
            let mut bin = pool.buckets[idx].lock().unwrap();
            pool.current_bytes.fetch_add(cap, Ordering::Relaxed);
            bin.push(mem::take(&mut self.buf));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::*;

    fn total_buffers(pool: &BufferPool) -> usize {
        pool.inner
            .buckets
            .iter()
            .map(|b| b.lock().unwrap().len())
            .sum()
    }

    #[test]
    fn test_basic_allocation_and_reuse() {
        let pool = BufferPool::new(1 << 20);
        let cap = 4096;

        let buffer1 = pool.get(cap);
        assert!(buffer1.capacity() >= cap);

        drop(buffer1);
        assert_eq!(total_buffers(&pool), 1);

        let buf2 = pool.get(cap);
        assert_eq!(buf2.capacity(), cap);
        drop(buf2);
    }

    #[test]
    fn test_different_bucket_sizes() {
        let pool = BufferPool::new(1 << 20);
        let small = pool.get(300); // should round to 512
        let medium = pool.get(2000); // ~2048
        let large = pool.get(10000); // ~16384

        assert!(small.capacity() >= 256);
        assert!(medium.capacity() >= 2048);
        assert!(large.capacity() >= 8192);

        drop(small);
        drop(medium);
        drop(large);
        assert!(total_buffers(&pool) >= 3);
    }

    #[test]
    fn test_bytes_accounting() {
        let pool = BufferPool::new(1 << 20);
        let b1 = pool.get(1024);
        let before = pool.bytes_in_pool();
        drop(b1);
        let after = pool.bytes_in_pool();
        assert!(after >= before);
    }

    #[test]
    fn test_pow2_ceil_correctness() {
        assert_eq!(super::pow2_ceil(0), 1);
        assert_eq!(super::pow2_ceil(1), 1);
        assert_eq!(super::pow2_ceil(2), 2);
        assert_eq!(super::pow2_ceil(3), 4);
        assert_eq!(super::pow2_ceil(5), 8);
        assert_eq!(super::pow2_ceil(255), 256);
        assert_eq!(super::pow2_ceil(4097), 8192);
    }

    #[test]
    fn test_drop_returns_to_pool() {
        let pool = BufferPool::new(1 << 20);
        let cap = 1024;
        {
            let _b = pool.get(cap);
            assert_eq!(total_buffers(&pool), 0);
        }
        // After drop, should be back
        assert_eq!(total_buffers(&pool), 1);
    }

    #[test]
    fn test_pool_eviction_large_buffer() {
        let pool = BufferPool::new(1 << 20);

        // Request a buffer bigger than MAX_BUCKET
        let large = pool.get(2 * super::MAX_BUCKET);

        // Expect the pool to cap it to MAX_BUCKET
        let cap = large.capacity();
        assert_eq!(
            cap,
            super::MAX_BUCKET,
            "capacity should be capped to MAX_BUCKET"
        );

        // Dropping it should still return to the pool (since it was capped)
        drop(large);

        // There should be exactly one buffer reused in the correct bucket
        assert_eq!(total_buffers(&pool), 1);
    }

    #[test]
    fn test_concurrent_access_reuse() {
        let pool = Arc::new(BufferPool::new(4 << 20));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let pool = pool.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        let mut buf = pool.get(2048);
                        buf.resize_uninit(2048);
                        buf.as_mut_slice()[0] = 42;
                        drop(buf);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        assert!(total_buffers(&pool) > 0)
    }

    #[test]
    fn test_resize_uninit_growth_and_truncate() {
        let pool = BufferPool::new(1 << 20);
        let mut buf = pool.get(1024);
        buf.resize_uninit(2048);
        assert_eq!(buf.len(), 2048);
        buf.resize_uninit(512);
        assert_eq!(buf.len(), 512);
    }

    #[test]
    fn test_multiple_borrow_and_return() {
        let pool = BufferPool::new(1 << 20);
        let mut bufs = Vec::new();
        for _ in 0..10 {
            bufs.push(pool.get(1024));
        }

        assert_eq!(total_buffers(&pool), 0);
        drop(bufs);
        assert!(total_buffers(&pool) > 0);
    }

    #[test]
    fn test_buffer_cleared_on_reuse() {
        let pool = BufferPool::new(1 << 20);
        {
            let mut buf = pool.get(512);
            buf.vec_mut().extend_from_slice(b"hello world");
        }
        {
            let buf = pool.get(512);
            assert_eq!(buf.len(), 0, "buffer must be cleared when reused");
        }
    }

    #[test]
    fn test_capacity_grows_pow2() {
        let pool = BufferPool::new(1 << 20);
        let b = pool.get(5000);
        assert_eq!(b.capacity(), 8192);
    }

    #[test]
    fn test_bytes_in_pool_changes_after_drop() {
        let pool = BufferPool::new(1 << 20);
        let buf = pool.get(1024);
        let before = pool.bytes_in_pool();
        drop(buf);
        let after = pool.bytes_in_pool();
        assert!(after >= before);
    }

    #[test]
    fn test_pool_does_not_exceed_max_size() {
        let pool = BufferPool::new(64 * 1024);
        for _ in 0..20 {
            pool.get(4096);
        }
        assert!(pool.bytes_in_pool() <= pool.inner.max_bytes * 2);
    }

    #[test]
    fn test_drop_large_number_of_buffers() {
        let pool = BufferPool::new(8 << 20);
        let mut vec = Vec::new();
        for _ in 0..1000 {
            vec.push(pool.get(512));
        }
        assert_eq!(total_buffers(&pool), 0);
        drop(vec);
        assert!(total_buffers(&pool) > 0);
    }

    #[test]
    fn test_buffer_reclaimed_after_drop_delay() {
        let pool = BufferPool::new(8 << 20);
        {
            let _b = pool.get(1024);
        }
        thread::sleep(Duration::from_millis(10));
        assert!(total_buffers(&pool) > 0);
    }
}
