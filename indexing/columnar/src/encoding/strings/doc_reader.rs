use std::{
    collections::{HashMap, HashSet},
    io,
};

use crate::{
    buffers::smart_pool::SmartBufferPool,
    encoding::{
        bitpack::v1::reader::decode_values,
        strings::common::{self, DOC_HEADER_SIZE},
    },
};

pub struct DocReader {
    pool: SmartBufferPool,
}

pub struct DocHeader {
    data_size: usize,
    entry_count: usize,
}

impl DocHeader {
    pub fn from_reader<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let mut buffer = [0u8; DOC_HEADER_SIZE];
        reader.read_exact(&mut buffer)?;

        if buffer[0..6] != *common::DOC_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid magic number {:?} != {:?}",
                    common::DOC_MAGIC,
                    &buffer[0..6]
                ),
            ));
        }
        let version = buffer[6];
        if version != common::DOC_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported version: {}", version),
            ));
        }
        let data_size = u32::from_le_bytes(buffer[7..11].try_into().unwrap()) as usize;
        let entry_count = u32::from_le_bytes(buffer[11..15].try_into().unwrap()) as usize;
        Ok(DocHeader {
            data_size,
            entry_count,
        })
    }
}

impl DocReader {
    pub fn new(pool: SmartBufferPool) -> Self {
        DocReader { pool }
    }

    pub fn search<R: io::Read>(&self, reader: &mut R, tokens: &[u64]) -> io::Result<bool> {
        if tokens.is_empty() {
            return Ok(false);
        }
        // Implementation goes here
        let header = DocHeader::from_reader(reader)?;
        let entry_size = header.entry_count * 16;
        let total_size = entry_size + header.data_size;
        let mut buffer = self.pool.get(total_size); // assumming we got the values.
        buffer.resize_uninit(total_size);
        reader.read_exact(&mut buffer.buf)?;

        let mut table = HashMap::with_capacity(header.entry_count);
        for i in 0..header.entry_count {
            let start = i * 16;
            let key = u64::from_le_bytes(buffer.buf[start..start + 8].try_into().unwrap());
            let offset = u64::from_le_bytes(buffer.buf[start + 8..start + 16].try_into().unwrap());
            table.insert(key, offset);
        }
        for &token in tokens {
            if !table.contains_key(&token) {
                return Ok(false);
            }
        }

        let sets = tokens
            .iter()
            .map(|value| -> io::Result<HashSet<u32>> {
                let offset = table
                    .get(value)
                    .ok_or(io::Error::new(io::ErrorKind::NotFound, "Not Found"))?;
                let size_start = entry_size + (*offset as usize);
                let size_end = size_start + 4;
                let buff_len =
                    u32::from_le_bytes(buffer.buf[size_start..size_end].try_into().unwrap());
                let width = buffer.buf[size_end];
                let buff_start = size_end + 1;
                let buff_end = buff_start + buff_len as usize;
                println!(
                    "Decoding token {} at offset {}: size {}, width {}, buffer [{}..{}]",
                    value, offset, buff_len, width, buff_start, buff_end
                );
                let decoded_values =
                    decode_values::<u32>(&buffer.buf[buff_start..buff_end], width)?;
                let result = decoded_values.into_iter().collect::<HashSet<_>>();
                Ok(result)
            })
            .collect::<io::Result<Vec<_>>>()?;

        let result = sets[0]
            .iter()
            .any(|val| (1..sets.len()).all(|i| sets[i].contains(&(val + i as u32))));
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::super::SmartBufferPool;
    use super::*;
    use crate::encoding::strings::common::DOC_MAGIC;
    use crate::encoding::strings::doc_writer::DocWriter;
    use std::io::Cursor;

    /// Helper function to perform a standard write-then-read-and-search test.
    fn run_search_test(doc_tokens: &[u64], search_tokens: &[u64], expected: bool) {
        // Arrange: Write the data
        let mut writer = DocWriter::default();
        let mut buffer = Vec::new();
        writer.write(doc_tokens, &mut buffer).unwrap();

        // Act: Read the data and search
        let pool = SmartBufferPool::new(1 << 20);
        let reader = DocReader::new(pool);
        let mut cursor = Cursor::new(buffer);
        let result = reader.search(&mut cursor, search_tokens);

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn test_basic_roundtrip_found() {
        run_search_test(&[10, 20, 30, 40], &[20, 30], true);
    }

    #[test]
    fn test_basic_roundtrip_not_found() {
        run_search_test(&[10, 20, 30, 40], &[20, 40], false);
    }

    #[test]
    fn test_sequence_at_start() {
        run_search_test(&[10, 20, 30, 40], &[10, 20, 30], true);
    }

    #[test]
    fn test_sequence_at_end() {
        run_search_test(&[10, 20, 30, 40], &[30, 40], true);
    }

    #[test]
    fn test_non_consecutive_search_fails() {
        // The search logic requires positions to be consecutive.
        run_search_test(&[10, 20, 30, 40], &[10, 30], false);
    }

    #[test]
    fn test_out_of_order_search_fails() {
        run_search_test(&[10, 20, 30, 40], &[30, 20], false);
    }

    #[test]
    fn test_search_for_missing_token() {
        // Fast path should return false if a token is not in the document at all.
        run_search_test(&[10, 20, 30, 40], &[20, 999], false);
    }

    #[test]
    fn test_with_duplicate_tokens_found() {
        // Search for `20, 30`. It appears starting at index 1.
        run_search_test(&[10, 20, 30, 10, 20], &[20, 30], true);
    }

    #[test]
    fn test_with_duplicate_tokens_found_at_second_occurrence() {
        // Search for `10, 50`. Occurs starting at index 4.
        run_search_test(&[10, 20, 30, 20, 10, 50], &[10, 50], true);
    }

    #[test]
    fn test_with_duplicate_tokens_complex_search() {
        // Doc:   apple, banana, grape, apple, banana, orange
        // Tokens: 10,    20,      30,    10,    20,      40
        // Search for `apple, banana` (10, 20). Should be true as it occurs twice.
        run_search_test(&[10, 20, 30, 10, 20, 40], &[10, 20], true);
    }

    #[test]
    fn test_with_duplicate_tokens_no_valid_sequence() {
        // Doc: apple, banana, apple, grape
        // Pos: 0,     1,      2,     3
        // Tokens: 10, 20, 10, 30
        // Search for `apple, grape` (10, 30).
        // `apple` is at {0, 2}. `grape` is at {3}.
        // Check 1: `val`=0. Is `0+1`=1 in grape's positions? No.
        // Check 2: `val`=2. Is `2+1`=3 in grape's positions? Yes. Returns true.
        run_search_test(&[10, 20, 10, 30], &[10, 30], true);
    }

    // --- Edge Case Tests ---
    #[test]
    fn test_empty_document_is_an_error() {
        // Arrange
        // An empty buffer represents a truncated file, not a valid "empty document".
        let buffer: Vec<u8> = Vec::new();
        let mut cursor = std::io::Cursor::new(buffer);

        let pool = SmartBufferPool::new(1 << 10);
        let reader = DocReader::new(pool);

        // Act
        let result = reader.search(&mut cursor, &[10, 20]);

        // Assert
        // We expect an error because read_exact cannot read the header.
        assert!(result.is_err());

        // For extra robustness, you can check the specific kind of error.
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_search_with_empty_tokens() {
        // Searching for nothing should trivially be true.
        run_search_test(&[10, 20, 30], &[], false);
    }

    #[test]
    fn test_empty_doc_and_empty_search() {
        run_search_test(&[], &[], false);
    }

    #[test]
    fn test_single_token_document_found() {
        run_search_test(&[100], &[100], true);
    }

    #[test]
    fn test_single_token_document_not_found() {
        run_search_test(&[100], &[200], false);
    }

    #[test]
    fn test_search_for_longer_sequence_than_doc() {
        run_search_test(&[10, 20], &[10, 20, 30], false);
    }

    // --- Error Handling Tests ---

    #[test]
    fn test_invalid_magic_number() {
        // Arrange
        let mut buffer = Vec::with_capacity(DOC_HEADER_SIZE);
        buffer.extend_from_slice(b"BADBOY");
        buffer.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0, 0]); // Rest of header
        buffer.resize(DOC_HEADER_SIZE, 0);
        let mut cursor = Cursor::new(buffer);
        let pool = SmartBufferPool::new(1 << 20);
        let reader = DocReader::new(pool);

        // Act
        let result = reader.search(&mut cursor, &[10]);

        // Assert
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("invalid magic number"));
    }

    #[test]
    fn test_unsupported_version() {
        // Arrange
        let mut buffer = DOC_MAGIC.to_vec();
        buffer.push(99); // Invalid version 99
        buffer.resize(DOC_HEADER_SIZE, 0);
        let mut cursor = Cursor::new(buffer);
        let pool = SmartBufferPool::new(1 << 20);
        let reader = DocReader::new(pool);

        // Act
        let result = reader.search(&mut cursor, &[10]);

        // Assert
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "unsupported version: 99");
    }
}
