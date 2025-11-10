use crate::encoding::iters::num::LeNum;
use core::mem::size_of;

pub const PAGE_MAGIC_BITPACK: &[u8; 6] = b"BITPK1";
pub const PAGE_VERSION: u8 = 1;

pub const PAGE_DEFAULT_SIZE: usize = 64 * 1024;
pub const PAGE_HEADER_SIZE: usize = 64;

/// Common interface for all integer types we want to bit-pack.
pub trait BitEncodable: LeNum + Sized + Copy + Ord {
    /// Number of bits for this type (e.g., 8 for u8, 64 for u64, platform for usize/isize).
    const BITS: u32;
    const MIN: Self;
    const MAX: Self;

    /// Encode the value to an unsigned `u64` payload using the type's canonical scheme:
    /// - Unsigned types: identity
    /// - Signed types: ZigZag (width-aware)
    fn encode(self) -> u64;
    /// Decode a value from the lower `BITS` bits of `payload` using the same scheme.
    fn decode(payload: u64) -> Self;
    // fn to_le_bytes(self) -> Vec<u8>;
    // fn from_le_bytes(slice: &[u8]) -> Self;
    /// A mask of the lower `BITS` bits.
    #[inline(always)]
    fn mask() -> u64 {
        if Self::BITS == 64 {
            u64::MAX
        } else {
            (1u64 << Self::BITS) - 1
        }
    }
}

#[inline(always)]
fn zigzag_encode_width_aware(n: i64, bits: u32) -> u64 {
    // ZigZag: (n << 1) ^ (n >> (bits-1))  // arithmetic shift for sign
    ((n << 1) ^ (n >> (bits - 1))) as u64
}

#[inline(always)]
fn zigzag_decode_u64(u: u64) -> i64 {
    // ZigZag inverse: (u >> 1) ^ -(u & 1)
    ((u >> 1) as i64) ^ (-((u & 1) as i64))
}

/* ---------- Unsigned impls: identity encode/decode ---------- */

macro_rules! impl_bitencodable_unsigned {
    ($($t:ty),*) => {
        $(
            impl BitEncodable for $t {
                const BITS: u32 = (size_of::<$t>() as u32) * 8;
                const MIN: $t = <$t>::MIN;
                const MAX: $t = <$t>::MAX;

                #[inline(always)]
                fn encode(self) -> u64 {
                    self as u64
                }

                #[inline(always)]
                fn decode(payload: u64) -> Self {
                    // Mask to the destination width and cast back
                    (payload & Self::mask()) as $t
                }
            }
        )*
    };
}

impl_bitencodable_unsigned!(u8, u16, u32, u64, usize);

/* ---------- Signed impls: ZigZag encode/decode ---------- */

macro_rules! impl_bitencodable_signed {
    ($($t:ty),*) => {
        $(
            impl BitEncodable for $t {
                const BITS: u32 = (size_of::<$t>() as u32) * 8;
                const MIN: $t = <$t>::MIN;
                const MAX: $t = <$t>::MAX;

                #[inline(always)]
                fn encode(self) -> u64 {
                    // width-aware ZigZag (so i8/i16/etc. don’t pay 64-bit sign cost)
                    zigzag_encode_width_aware(self as i64, Self::BITS)
                }

                #[inline(always)]
                fn decode(payload: u64) -> Self {
                    // Only look at the bits that belong to this type
                    let u = payload & Self::mask();
                    zigzag_decode_u64(u) as $t
                }
            }
    )*
    };
}

impl_bitencodable_signed!(i8, i16, i32, i64, isize);

/* ---------- Helpers you can reuse with any BitEncodable ---------- */

/// Minimal bit width needed to store `value` after encoding.
/// (Returns 0 for 0.)
#[inline(always)]
pub fn bit_width_from_value<T: BitEncodable>(value: T) -> u8 {
    let enc = value.encode();
    if enc == 0 {
        1
    } else {
        (64 - enc.leading_zeros()) as u8
    }
}

/// Clamp a requested width to the type's maximum width.
#[inline(always)]
pub fn clamp_width_to_type<T: BitEncodable>(width: u8) -> u8 {
    width.min(T::BITS as u8)
}
