use crate::encoding::bitpack::v1::common::{
    BitEncodable, bit_width_from_value, clamp_width_to_type,
};
use std::io::{self, Write};
use std::marker::PhantomData;

/// Writes bit-packed values of type T into a `Write` stream.
/// This implementation is the symmetrical inverse of the BitReader.
pub struct BitWriter<W: Write, T: BitEncodable> {
    writer: W,
    bits: u64,     // 64-bit buffer, mirroring BitReader
    bit_count: u8, // Number of valid bits in the buffer
    width: u8,     // Bits per value
    _marker: std::marker::PhantomData<T>,
}

impl<W: Write, T: BitEncodable> BitWriter<W, T> {
    /// Create a writer with fixed width `width` bits per value.
    pub fn new(writer: W, width: u8) -> Self {
        let width = clamp_width_to_type::<T>(width);
        Self {
            writer,
            bits: 0,
            bit_count: 0,
            width,
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a writer by deriving width from `max_value`.
    pub fn from_max_value(writer: W, max_value: T) -> Self {
        let width = bit_width_from_value(max_value);
        Self::new(writer, width)
    }

    /// Write a single value of type `T`.
    pub fn write_value(&mut self, value: T) -> io::Result<()> {
        let mut encoded = value.encode();
        let mut bits_to_write = self.width;

        if bits_to_write == 0 {
            return Ok(());
        }

        // Loop until all bits of the value are added to the buffer.
        // This correctly handles cases where a value is larger than
        // the remaining space in the 'bits' buffer.
        while bits_to_write > 0 {
            let space = 64 - self.bit_count;
            let chunk_size = bits_to_write.min(space);

            if chunk_size > 0 {
                let mask = if chunk_size == 64 {
                    u64::MAX
                } else {
                    (1 << chunk_size) - 1u64
                };

                // Add the next chunk of bits from the value into our buffer.
                self.bits |= (encoded & mask) << self.bit_count;

                self.bit_count += chunk_size;
                encoded >>= chunk_size;
                bits_to_write -= chunk_size;
            }

            // Flush all complete bytes from the buffer to the underlying writer.
            while self.bit_count >= 8 {
                self.writer.write_all(&[self.bits as u8])?;
                self.bits >>= 8;
                self.bit_count -= 8;
            }
        }
        Ok(())
    }

    /// Write multiple values from an iterator.
    pub fn write_all_values<I>(&mut self, values: I) -> io::Result<()>
    where
        I: IntoIterator<Item = T>,
    {
        for v in values {
            self.write_value(v)?;
        }
        Ok(())
    }

    /// Flush remaining bits (pad with zeros).
    pub fn flush(&mut self) -> io::Result<()> {
        if self.bit_count > 0 {
            self.writer.write_all(&[self.bits as u8])?;
            self.bits = 0;
            self.bit_count = 0;
        }
        self.writer.flush()
    }
}

impl<W: Write, T: BitEncodable> Drop for BitWriter<W, T> {
    fn drop(&mut self) {
        // Attempt to flush remaining bits, but ignore errors as we can't panic in drop.
        let _ = self.flush();
    }
}

//=============================================================================
// 2. The Borrowing Version (BitWriterRef)
//    - Borrows its writer `&'a mut W`.
//    - DOES implement Drop to automatically flush.
//=============================================================================

pub struct BitWriterRef<'a, W: Write, T: BitEncodable> {
    writer: &'a mut W,
    bits: u64,
    bit_count: u8,
    width: u8,
    _marker: PhantomData<T>,
}

impl<'a, W: Write, T: BitEncodable> BitWriterRef<'a, W, T> {
    pub fn new(writer: &'a mut W, width: u8) -> Self {
        let width = clamp_width_to_type::<T>(width);
        Self {
            writer,
            bits: 0,
            bit_count: 0,
            width,
            _marker: PhantomData,
        }
    }

    // The flush and write_value methods are identical to the owning version,
    // just operating on `self.writer` which is a `&mut W`.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.bit_count > 0 {
            while self.bit_count >= 8 {
                self.writer.write_all(&[self.bits as u8])?;
                self.bits >>= 8;
                self.bit_count -= 8;
            }
            if self.bit_count > 0 {
                self.writer.write_all(&[self.bits as u8])?;
                self.bits = 0;
                self.bit_count = 0;
            }
        }
        self.writer.flush()
    }

    pub fn write_value(&mut self, value: T) -> io::Result<()> {
        let mut encoded = value.encode();
        let mut bits_to_write = self.width;

        while bits_to_write > 0 {
            let space = 64 - self.bit_count;
            let chunk_size = bits_to_write.min(space);
            if chunk_size > 0 {
                let mask = if chunk_size == 64 {
                    u64::MAX
                } else {
                    (1 << chunk_size) - 1
                };
                self.bits |= (encoded & mask) << self.bit_count;
                self.bit_count += chunk_size;
                encoded >>= chunk_size;
                bits_to_write -= chunk_size;
            }
            while self.bit_count >= 8 {
                self.writer.write_all(&[self.bits as u8])?;
                self.bits >>= 8;
                self.bit_count -= 8;
            }
        }
        Ok(())
    }
}

/// The borrowing writer automatically flushes when it goes out of scope.
impl<'a, W: Write, T: BitEncodable> Drop for BitWriterRef<'a, W, T> {
    fn drop(&mut self) {
        // Ignore errors in drop, as we cannot panic.
        let _ = self.flush();
    }
}

pub fn encode_values<T: BitEncodable>(values: &[T]) -> io::Result<(u8, Vec<u8>)> {
    if values.is_empty() {
        return Ok((0, Vec::new()));
    }
    let max_value = values.iter().cloned().max().unwrap();
    let len = values.len() as u32;
    let width = bit_width_from_value(max_value);
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&len.to_le_bytes());
    {
        let mut writer = BitWriter::new(&mut buffer, width);
        for v in values {
            writer.write_value(*v)?;
        }
        writer.flush()?;
    }
    Ok((width, buffer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::bitpack::v1::reader::decode_values;

    #[test]
    fn test_values() {
        let bytes = [
            0, 0, 0, 0, 0, 0, 0, 128, 0, 0, 0, 0, 0, 0, 0, 192, 255, 255, 255, 255, 255, 255, 255,
            31,
        ];

        let mut stream = &bytes[..];
        let mut bits = 0u64;
        let mut bit_count = 0u8;

        let mut read_bits = |n: u8| -> u64 {
            while bit_count < n {
                let byte = stream[0];
                stream = &stream[1..];
                bits |= (byte as u64) << bit_count;
                bit_count += 8;
            }
            let mask = if n == 64 { !0 } else { (1u64 << n) - 1 };
            let val = bits & mask;
            bits >>= n;
            bit_count -= n;
            // Critical: mask high bits to avoid garbage
            if bit_count > 0 {
                bits &= (1u64 << bit_count) - 1;
            }
            val
        };

        let v0 = read_bits(63);
        let v1 = read_bits(63);
        let v2 = read_bits(63);

        println!("v0 = {}", v0);
        println!("v1 = {}", v1);
        println!("v2 = {}", v2);
    }

    #[test]
    fn test_encode_values() {
        let values: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 63, 64, 127, 128, 255, 256, 511, 512, 1023];
        let (width, encoded) = encode_values(&values.clone()).unwrap();
        let decoded = decode_values(&encoded, width).unwrap();
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_encode_values_single() {
        let values: Vec<u32> = vec![0];
        let (width, encoded) = encode_values(&values.clone()).unwrap();
        let decoded = decode_values(&encoded, width).unwrap();
        assert_eq!(values, decoded);
    }
}
