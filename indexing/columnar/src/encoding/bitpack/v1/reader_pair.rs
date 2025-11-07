use crate::encoding::bitpack::v1::common::{BitEncodable, bit_width_from_value};
use crate::encoding::bitpack::v1::reader::BitReader;
use std::io::{self, Read};

pub struct PairBitReader<R: Read, A: BitEncodable, B: BitEncodable> {
    reader: BitReader<R>,
    width_a: u8,
    width_b: u8,
    _marker_a: std::marker::PhantomData<A>,
    _marker_b: std::marker::PhantomData<B>,
}

impl<R: Read, A: BitEncodable, B: BitEncodable> PairBitReader<R, A, B> {
    /// Create from known bit widths.
    pub fn new(reader: R, width_a: u8, width_b: u8) -> Self {
        Self {
            reader: BitReader::new(reader),
            width_a,
            width_b,
            _marker_a: std::marker::PhantomData,
            _marker_b: std::marker::PhantomData,
        }
    }

    /// Derive bit widths from maximum values.
    pub fn from_max_values(reader: R, max_a: A, max_b: B) -> Self {
        let width_a = bit_width_from_value(max_a);
        let width_b = bit_width_from_value(max_b);
        Self::new(reader, width_a, width_b)
    }

    /// Read a single pair of values.
    pub fn read_pair(&mut self) -> io::Result<Option<(A, B)>> {
        // Attempt to read value A
        let val_a = match self.reader.read_bits(self.width_a) {
            Ok(bits_a) => A::decode(bits_a),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };

        // Attempt to read value B
        let val_b = match self.reader.read_bits(self.width_b) {
            Ok(bits_b) => B::decode(bits_b),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete data for value B",
                ));
            }
            Err(e) => return Err(e),
        };

        Ok(Some((val_a, val_b)))
    }
}

/// Decodes a stream of bit-packed pairs into a vector.
pub fn decode_pairs<R: Read, A: BitEncodable, B: BitEncodable>(
    reader: R,
    max_a: A,
    max_b: B,
    num_pairs: usize,
) -> io::Result<Vec<(A, B)>> {
    let mut pair_reader = PairBitReader::from_max_values(reader, max_a, max_b);
    let mut result = Vec::with_capacity(num_pairs);

    for _ in 0..num_pairs {
        match pair_reader.read_pair()? {
            Some(pair) => result.push(pair),
            None => break, // clean EOF
        }
    }

    Ok(result)
}
