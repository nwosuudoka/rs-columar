use crate::encoding::bitpack::v1::common::BitEncodable;
use std::io::{self, Read};

const BUF_SIZE: usize = 512;

/// Reads bit-packed integers from any `Read`.
pub struct BitReader<R: Read> {
    reader: R,
    buf: [u8; BUF_SIZE],
    pos: usize,
    end: usize,
    bits: u64,
    bit_count: u8,
}

impl<R: Read> BitReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buf: [0; BUF_SIZE],
            pos: 0,
            end: 0,
            bits: 0,
            bit_count: 0,
        }
    }

    /// Reads `width` bits from the stream. `width` must be <= 64.
    pub fn read_bits(&mut self, width: u8) -> io::Result<u64> {
        if width == 0 {
            return Ok(0);
        }
        if width > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bit width > 64",
            ));
        }

        // Fast path: if we already have enough bits in the buffer.
        if self.bit_count >= width {
            let mask = if width == 64 {
                u64::MAX
            } else {
                (1u64 << width) - 1
            };
            let val = self.bits & mask;
            self.bits >>= width;
            self.bit_count -= width;
            return Ok(val);
        }

        // Not enough bits. Assemble the result piece by piece.
        // Start with the bits we already have.
        let mut result = self.bits;
        let mut bits_read = self.bit_count;

        // Clear the buffer since we've consumed it.
        self.bits = 0;
        self.bit_count = 0;

        // Read bytes from the underlying reader until we have enough bits.
        while bits_read < width {
            if self.pos == self.end {
                self.end = self.reader.read(&mut self.buf)?;
                self.pos = 0;
                if self.end == 0 {
                    // We hit EOF but didn't get enough bits for the requested width.
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "not enough bits",
                    ));
                }
            }

            let next_byte = self.buf[self.pos] as u64;
            self.pos += 1;

            let bits_needed = width - bits_read;
            let bits_from_byte = bits_needed.min(8);

            // Append the needed bits from the new byte to our result.
            result |= (next_byte & ((1u64 << bits_from_byte) - 1)) << bits_read;
            bits_read += bits_from_byte;

            // If we didn't use the whole byte, put the remainder in the buffer
            // for the next read.
            if bits_from_byte < 8 {
                self.bit_count = 8 - bits_from_byte;
                self.bits = next_byte >> bits_from_byte;
            }
        }
        Ok(result)
    }

    pub fn read_value<T: BitEncodable>(&mut self, width: u8) -> io::Result<T> {
        let raw = self.read_bits(width)?;
        Ok(T::decode(raw))
    }
}

/* -------- Iterator wrapper -------- */

/// Iterator over bit-packed values of type `T`.
/// Can be bounded (known count) or unbounded (until EOF).
pub struct BitStream<R: Read, T: BitEncodable> {
    reader: BitReader<R>,
    width: u8,
    remaining: Option<usize>,
    _marker: std::marker::PhantomData<T>,
}

impl<R: Read, T: BitEncodable> BitStream<R, T> {
    /// Reads `count` values of `width` bits each.
    pub fn with_count(reader: R, width: u8, count: usize) -> Self {
        Self {
            reader: BitReader::new(reader),
            width,
            remaining: Some(count),
            _marker: std::marker::PhantomData,
        }
    }

    /// Reads values of `width` bits until EOF.
    pub fn new(reader: R, width: u8) -> Self {
        Self {
            reader: BitReader::new(reader),
            width,
            remaining: None,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<R: Read, T: BitEncodable> Iterator for BitStream<R, T> {
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        // If fixed count, stop when exhausted
        if let Some(ref mut rem) = self.remaining {
            if *rem == 0 {
                return None;
            }
            *rem -= 1;
        }

        match self.reader.read_value::<T>(self.width) {
            Ok(v) => Some(Ok(v)),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None, // clean EOF
            Err(e) => Some(Err(e)),
        }
    }
}

pub fn decode_values<T: BitEncodable>(reader: &[u8], width: u8) -> io::Result<Vec<T>> {
    let count = u32::from_le_bytes(reader[0..4].try_into().unwrap()) as usize;
    let bit_reader = BitStream::with_count(io::Cursor::new(&reader[4..]), width, count);
    bit_reader.collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::bitpack::v1::{
        common::{bit_width_from_value, clamp_width_to_type},
        writer::BitWriter,
    };
    use std::io::Cursor;

    fn roundtrip_generic<T: BitEncodable + PartialEq + std::fmt::Debug>(
        values: &[T],
        use_count: bool,
    ) {
        let max = *values
            .iter()
            .max_by(|a, b| a.encode().cmp(&b.encode()))
            .unwrap();
        let width = clamp_width_to_type::<T>(bit_width_from_value(max));

        // Encode into a memory buffer
        let mut encoded = Vec::new();
        {
            let mut writer = BitWriter::<_, T>::new(&mut encoded, width);
            writer.write_all_values(values.iter().copied()).unwrap();
            writer.flush().unwrap();
        }

        // Decode from the encoded buffer
        let cursor = Cursor::new(&encoded);
        let decoded: Vec<T> = if use_count {
            // Bounded mode: stop after N values
            BitStream::<_, T>::with_count(cursor, width, values.len())
                .map(|r| r.unwrap())
                .collect()
        } else {
            // Unbounded mode: read until EOF
            BitStream::<_, T>::new(cursor, width)
                .map(|r| r.unwrap())
                .collect()
        };

        assert_eq!(
            values,
            &decoded[..],
            "mode={use_count}, type={}",
            std::any::type_name::<T>()
        );
    }

    #[test]
    fn test_bitstream_modes_unsigned() {
        // Simple unsigned values
        let values_u8 = vec![0u8, 1, 5, 42, 127, 255];
        let values_u32 = vec![0u32, 1_000, 50_000, 1_000_000];

        // Test both modes
        roundtrip_generic(&values_u8, true); // with count
        roundtrip_generic(&values_u8, false); // until EOF

        roundtrip_generic(&values_u32, true);
        roundtrip_generic(&values_u32, false);
    }

    #[test]
    fn test_bitstream_modes_signed() {
        let values_i16 = vec![-300, -2, -1, 0, 1, 2, 32767];
        let values_i64: Vec<i64> = vec![-10_000_000_000, -1, 0, 1, 10_000_000_000];

        roundtrip_generic(&values_i16, true);
        roundtrip_generic(&values_i16, false);

        roundtrip_generic(&values_i64, true);
        roundtrip_generic(&values_i64, false);
    }

    #[test]
    fn test_unbounded_empty_input() {
        let encoded: Vec<u8> = vec![];
        let cursor = Cursor::new(&encoded);
        let decoded: Vec<i32> = BitStream::<_, i32>::new(cursor, 8)
            .map(|r| r.unwrap())
            .collect();
        assert!(decoded.is_empty());
    }
}
