use crate::encoding::bitpack::v1::common::{
    BitEncodable, bit_width_from_value, clamp_width_to_type,
};
use std::io::{self, Write};

/// Writes pairs of values (T, U) bit-packed with fixed widths.
pub struct PairBitWriter<W: Write, A: BitEncodable, B: BitEncodable> {
    writer: W,
    current_byte: u8,
    bit_count: u8,
    width_a: u8,
    width_b: u8,
    _marker_a: std::marker::PhantomData<A>,
    _marker_b: std::marker::PhantomData<B>,
}

impl<W: Write, A: BitEncodable, B: BitEncodable> PairBitWriter<W, A, B> {
    /// Create with explicit widths.
    pub fn new(writer: W, width_a: u8, width_b: u8) -> Self {
        let wa = clamp_width_to_type::<A>(width_a);
        let wb = clamp_width_to_type::<B>(width_b);
        Self {
            writer,
            current_byte: 0,
            bit_count: 0,
            width_a: wa,
            width_b: wb,
            _marker_a: std::marker::PhantomData,
            _marker_b: std::marker::PhantomData,
        }
    }

    /// Derive widths from maximum values.
    pub fn from_max_values(writer: W, max_a: A, max_b: B) -> Self {
        let width_a = bit_width_from_value(max_a);
        let width_b = bit_width_from_value(max_b);
        Self::new(writer, width_a, width_b)
    }

    #[inline(always)]
    fn write_bit(&mut self, bit: bool) -> io::Result<()> {
        if bit {
            self.current_byte |= 1 << self.bit_count;
        }
        self.bit_count += 1;
        if self.bit_count == 8 {
            self.writer.write_all(&[self.current_byte])?;
            self.current_byte = 0;
            self.bit_count = 0;
        }
        Ok(())
    }

    /// Write one pair (a, b).
    pub fn write_pair(&mut self, a: A, b: B) -> io::Result<()> {
        let enc_a = a.encode();
        let enc_b = b.encode();
        for i in 0..(self.width_a as usize) {
            let bit = ((enc_a >> i) & 1) == 1;
            self.write_bit(bit)?;
        }
        for i in 0..(self.width_b as usize) {
            let bit = ((enc_b >> i) & 1) == 1;
            self.write_bit(bit)?;
        }
        Ok(())
    }

    /// Flush remaining bits (pad with zeros).
    pub fn flush(&mut self) -> io::Result<()> {
        if self.bit_count > 0 {
            self.writer.write_all(&[self.current_byte])?;
            self.current_byte = 0;
            self.bit_count = 0;
        }
        self.writer.flush()
    }
}

impl<W: Write, A: BitEncodable, B: BitEncodable> Drop for PairBitWriter<W, A, B> {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Convenience function to encode a slice of pairs into a `Vec<u8>`.
pub fn encode_pairs<A: BitEncodable, B: BitEncodable>(
    pairs: &[(A, B)],
    max_a: A,
    max_b: B,
) -> io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    {
        let mut writer = PairBitWriter::from_max_values(&mut buffer, max_a, max_b);
        for &(ref a, ref b) in pairs {
            writer.write_pair(*a, *b)?;
        }
        writer.flush()?;
    }
    Ok(buffer)
}
