use crate::table::common::IsAllowedId;

use super::common::{OffsetHeader, ROW_OFFSET_SIZE};
use super::reader_source::ReaderSource;
use std::io::{self, BufReader, ErrorKind, Read, Result, Seek, Take};

#[derive(Debug, Clone)]
pub struct KeyEntry<T: IsAllowedId> {
    pub id: T,
    pub row_offset: u64,
}

#[derive(Debug)]
pub struct SectionReader<'a> {
    reader: BufReader<Take<&'a mut ReaderSource>>,
}

impl<'a> Read for SectionReader<'a> {
    /*************  ✨ Windsurf Command ⭐  *************/
    /// Reads from the underlying reader into a provided buffer.
    ///
    /// Returns the number of bytes read, or an error if the operation fails.
    ///
    /// This function is a wrapper around the `Read::read` method of the underlying reader.
    /// As such, it will return an error if the underlying reader is at the end of
    /// the file, or if an I/O error occurs.
    /*******  da248dea-5432-4b82-9ce3-dff903d5c327  *******/
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.reader.read(buf)
    }
}

#[derive(Debug)]
pub struct KeyReader<T: IsAllowedId> {
    entries: Vec<KeyEntry<T>>,
    reader: ReaderSource,
}

// In your KeyReader impl block
impl<T: IsAllowedId> KeyReader<T> {
    pub fn new(entries: Vec<KeyEntry<T>>, reader: ReaderSource) -> Self {
        KeyReader { entries, reader }
    }
    /// The public API method for iteration.
    /// Its job is to handle the iteration protocol (when to stop)
    /// and delegate the complex work.
    pub fn next_reader<'a>(&'a mut self) -> Option<io::Result<SectionReader<'a>>> {
        if self.entries.is_empty() {
            return None;
        }

        // Delegate the actual processing to the private helper.
        // We wrap its Result in Some() to match the iterator return type.
        Some(self.process_next_entry())
    }

    /// This private helper contains the core logic for processing one entry.
    /// Because it returns a Result directly, we can use the `?` operator inside it
    /// for clean, linear error handling.
    fn process_next_entry<'a>(&'a mut self) -> io::Result<SectionReader<'a>> {
        let entry = self.entries.remove(0);

        // --- Find search area bounds ---
        self.reader.seek(io::SeekFrom::Start(entry.row_offset))?;
        let mut buffer = [0u8; ROW_OFFSET_SIZE];
        self.reader.read_exact(&mut buffer)?;
        let data_offset = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
        let row_count = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
        let row_byte_size = (OffsetHeader::<T>::size()) as u32 * row_count;

        // --- Perform the search ---
        let found_header = {
            self.reader.seek(io::SeekFrom::Start(data_offset))?;
            let search_limit = (&mut self.reader).take(row_byte_size as u64);
            let mut search_reader = BufReader::new(search_limit);
            // The `?` here cleanly propagates any I/O error or "Not Found" error
            // from the find_header_by_id function.
            find_header_by_id(&mut search_reader, entry.id)?
        };

        // If we get here, the header was found successfully. The `?` operator
        // handled the error case for us.

        // --- Reset the underlying file's offset and create the SectionReader ---
        self.reader
            .seek(io::SeekFrom::Start(found_header.offset as u64))?;
        let final_reader = (&mut self.reader).take(found_header.size as u64);

        // Return Ok with the safe, temporary SectionReader.
        Ok(SectionReader {
            reader: BufReader::new(final_reader),
        })
    }
}

fn find_header_by_id<R: Read, T: IsAllowedId>(
    reader: &mut R,
    target_id: T,
) -> io::Result<OffsetHeader<T>> {
    let struct_size = OffsetHeader::<T>::size();
    const MAX_STRUCT_SIZE: usize = 8 + 8 + 4;
    let mut buffer = [0u8; MAX_STRUCT_SIZE];
    let active_slice = &mut buffer[0..struct_size];

    loop {
        match reader.read_exact(active_slice) {
            Ok(()) => {
                let header = OffsetHeader::from_buffer(active_slice).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("error reading from buffer {}", e),
                    )
                })?;
                if header.id == target_id {
                    return Ok(header);
                }
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                return Err(io::Error::new(
                    ErrorKind::NotFound,
                    "Header not found for entry",
                ));
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};

    fn write_at(vec: &mut Vec<u8>, offset: usize, data: &[u8]) {
        vec[offset..(offset + data.len())].copy_from_slice(data);
    }

    fn write_header<T: IsAllowedId>(value: &OffsetHeader<T>) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.extend_from_slice(&value.offset.to_le_bytes());
        vec.extend_from_slice(&value.id.get_le_bytes());
        vec.extend_from_slice(&value.size.to_le_bytes());
        vec
    }

    #[test]
    fn test_find_header_by_id() {
        let mut binary_data = vec![0u8; 2000];

        const META_1_OFFSET: u64 = 100;
        const META_2_OFFSET: u64 = 116;
        const META_3_OFFSET: u64 = 132; // This will point to a block where the header is missing.

        const HEADER_BLOCK_1_OFFSET: u64 = 500;
        const HEADER_BLOCK_2_OFFSET: u64 = 600;

        const DATA_1_OFFSET: u64 = 1000;
        const DATA_2_OFFSET: u64 = 1100;

        // Write the final data sections the user wants to read.
        write_at(&mut binary_data, DATA_1_OFFSET as usize, b"Hello, Rust!"); // 12 bytes
        write_at(&mut binary_data, DATA_2_OFFSET as usize, b"Iterator Test"); // 13 bytes

        let header1 = OffsetHeader::<u32> {
            offset: DATA_1_OFFSET,
            id: 101,
            size: 12,
        };
        let dummy_header = OffsetHeader::<u32> {
            offset: 0,
            id: 999,
            size: 0,
        }; // A distractor
        write_at(
            &mut binary_data,
            HEADER_BLOCK_1_OFFSET as usize,
            &write_header(&header1),
        );
        write_at(
            &mut binary_data,
            (HEADER_BLOCK_1_OFFSET + 16) as usize,
            &write_header(&dummy_header),
        );

        // Header Block 2 (for ID 202)
        let header2 = OffsetHeader {
            offset: DATA_2_OFFSET,
            id: 202_u32,
            size: 13,
        };
        write_at(
            &mut binary_data,
            HEADER_BLOCK_2_OFFSET as usize,
            &write_header(&header2),
        );

        // Write the Metadata Blocks, which point to the Header Blocks.
        // Each metadata block is 16 bytes (u64 offset_to_header_block, u64 size_of_header_block).
        write_at(
            &mut binary_data,
            META_1_OFFSET as usize,
            &HEADER_BLOCK_1_OFFSET.to_le_bytes(),
        );
        write_at(
            &mut binary_data,
            (META_1_OFFSET + 8) as usize,
            &32u64.to_le_bytes(),
        ); // Size is 2 headers

        write_at(
            &mut binary_data,
            META_2_OFFSET as usize,
            &HEADER_BLOCK_2_OFFSET.to_le_bytes(),
        );
        write_at(
            &mut binary_data,
            (META_2_OFFSET + 8) as usize,
            &16u64.to_le_bytes(),
        ); // Size is 1 header

        // Metadata for the failing test case (points to a valid block, but the ID we search for won't be in it)
        write_at(
            &mut binary_data,
            META_3_OFFSET as usize,
            &HEADER_BLOCK_1_OFFSET.to_le_bytes(),
        );
        write_at(
            &mut binary_data,
            (META_3_OFFSET + 8) as usize,
            &32u64.to_le_bytes(),
        );

        // --- 2. EXECUTION: Create the KeyReader and iterate ---

        // Define the entries our KeyReader will process, in order.
        let entries = vec![
            KeyEntry {
                id: 101_u32,
                row_offset: META_1_OFFSET,
            },
            KeyEntry {
                id: 202_u32,
                row_offset: META_2_OFFSET,
            },
            KeyEntry {
                id: 555_u32,
                row_offset: META_3_OFFSET,
            }, // This ID doesn't exist in the block
        ];

        let reader_source = ReaderSource::Cursor(Cursor::new(binary_data));
        let mut key_reader = KeyReader::new(entries, reader_source);

        // --- 3. ASSERTIONS: Verify each step of the iteration ---

        // -- First call to next_reader() should succeed and give us "Hello, Rust!" --
        let result1 = key_reader.next_reader();
        assert!(
            result1.is_some(),
            "Expected a reader for the first entry, but got None"
        );

        let mut reader1 = result1
            .unwrap()
            .expect("First entry should have processed successfully");
        let mut content1 = String::new();
        reader1
            .read_to_string(&mut content1)
            .expect("Reading from first section should succeed");
        assert_eq!(content1, "Hello, Rust!");

        // -- Second call should succeed and give us "Iterator Test" --
        let result2 = key_reader.next_reader();
        assert!(
            result2.is_some(),
            "Expected a reader for the second entry, but got None"
        );

        let mut reader2 = result2
            .unwrap()
            .expect("Second entry should have processed successfully");
        let mut content2 = String::new();
        reader2
            .read_to_string(&mut content2)
            .expect("Reading from second section should succeed");
        assert_eq!(content2, "Iterator Test");

        // -- Third call should fail with a "Not Found" error --
        let result3 = key_reader.next_reader();
        assert!(
            result3.is_some(),
            "Expected a result for the third entry, but got None"
        );

        let err = result3
            .unwrap()
            .expect_err("Third entry should have failed to find the header");
        assert_eq!(err.kind(), ErrorKind::NotFound);

        // -- Fourth call should return None, signaling the end of the iteration --
        let result4 = key_reader.next_reader();
        assert!(
            result4.is_none(),
            "Expected iteration to be finished, but got another result"
        );
    }
}
