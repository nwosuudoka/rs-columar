pub mod buffer_pool;
pub mod errors;
pub mod smart_pool;

#[inline]
pub(crate) fn pow2_ceil(mut n: usize) -> usize {
    if n <= 1 {
        return 1;
    }
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n += 1;
    n
}
