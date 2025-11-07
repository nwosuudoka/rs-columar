mod tests {
    use crate::encoding::bitpack::v1::{
        common::{BitEncodable, bit_width_from_value, clamp_width_to_type},
        reader::{BitReader, BitStream},
        reader_pair::decode_pairs,
        writer::BitWriter,
        writer_pair::encode_pairs,
    };
    use std::io::Cursor;

    #[test]
    fn roundtrip_basic_integers() {
        fn roundtrip<T: BitEncodable + PartialEq + std::fmt::Debug>(values: &[T]) {
            let max = *values
                .iter()
                .max_by(|a, b| a.encode().cmp(&b.encode()))
                .unwrap();
            let width = bit_width_from_value(max);
            let mut encoded = Vec::new();

            // Encode
            {
                let mut writer = BitWriter::<_, T>::new(&mut encoded, width);
                writer.write_all_values(values.iter().copied()).unwrap();
                writer.flush().unwrap();
            }

            // Decode
            let cursor = Cursor::new(&encoded);
            // println!("{:?}", encoded.iter().map(|v| *v).collect::<Vec<u8>>());
            let decoded: Vec<T> = BitStream::<_, T>::with_count(cursor, width, values.len())
                .map(|r| r.unwrap())
                .collect();

            assert_eq!(values, &decoded[..]);
        }

        // roundtrip(&[0u8, 1, 2, 3, 4, 5, 255]);
        // roundtrip(&[0u16, 42, 65535]);
        // roundtrip(&[0u32, 1_000, 100_000]);
        // roundtrip(&[0i8, -1, 1, -5, 5, i8::MIN + 1, i8::MAX]);
        // roundtrip(&[0i16, -1234, 1234, i16::MIN + 1, i16::MAX]);
        // roundtrip(&[0i32, -1_000_000, 1_000_000]);
        // roundtrip(&[0i64, -10_000_000_000, 10_000_000_000]);
        // roundtrip(&[0i64, 1, (i64::MAX / 3)]);
        // roundtrip(&[0u64, 1, (u64::MAX / 3)]);
        roundtrip(&[0i64, 1, (i64::MAX / 2), 3]); // investigate this large number
        // roundtrip(&[0i64, 1, (u64::MAX / 2), 3]); // investigate this large number
    }

    #[test]
    fn roundtrip_variable_widths() {
        // Each value gets its own width derived from its max.
        for val in [1i32, 15, 255, 1023, 4095, 65535] {
            let width = bit_width_from_value(val);
            let mut encoded = Vec::new();

            {
                let mut writer = BitWriter::<_, i32>::new(&mut encoded, width);
                writer.write_value(val).unwrap();
                writer.flush().unwrap();
            }

            let mut reader = BitReader::new(Cursor::new(encoded));
            let got: i32 = reader.read_value(width).unwrap();
            assert_eq!(val, got);
        }
    }

    #[test]
    fn roundtrip_pairs_signed_unsigned() {
        let pairs: Vec<(i16, u16)> = vec![(-10, 10), (0, 0), (100, 500), (-32768, 65535)];
        let max_a = pairs
            .iter()
            .map(|(a, _)| a.checked_abs().unwrap_or_else(|| i16::MAX))
            .max()
            .unwrap();
        let max_b = *pairs.iter().map(|(_, b)| b).max().unwrap();

        let encoded = encode_pairs(&pairs, max_a, max_b).unwrap();
        let reader = Cursor::new(&encoded);
        let decoded = decode_pairs(reader, max_a, max_b, pairs.len()).unwrap();

        assert_eq!(pairs, decoded);
    }

    #[test]
    fn bit_width_sanity() {
        assert_eq!(bit_width_from_value(0u8), 1);
        assert_eq!(bit_width_from_value(1u8), 1);
        assert_eq!(bit_width_from_value(255u8), 8);
        assert_eq!(bit_width_from_value(256u16), 9);
        assert_eq!(bit_width_from_value(-1i16), 1); // ZigZag(−1) = 1
    }

    #[test]
    fn clamp_width_does_not_exceed_type_bits() {
        assert_eq!(clamp_width_to_type::<u8>(10), 8);
        assert_eq!(clamp_width_to_type::<i16>(20), 16);
        assert_eq!(clamp_width_to_type::<u64>(64), 64);
        assert_eq!(clamp_width_to_type::<u64>(65), 64);
    }

    fn zigzag_encode_width_aware(n: i64, bits: u32) -> u64 {
        // ZigZag: (n << 1) ^ (n >> (bits-1))  // arithmetic shift for sign
        ((n << 1) ^ (n >> (bits - 1))) as u64
    }

    #[inline(always)]
    fn zigzag_decode_u64(u: u64) -> i64 {
        // ZigZag inverse: (u >> 1) ^ -(u & 1)
        ((u >> 1) as i64) ^ (-((u & 1) as i64))
    }

    #[test]
    fn test_encode_numbers() {
        let v = zigzag_encode_width_aware(i64::MAX / 2, 63);
        println!("encoded: {}", v);
        println!("decoded: {}", zigzag_decode_u64(v));
    }
}
