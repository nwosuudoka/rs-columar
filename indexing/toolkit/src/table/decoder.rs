use crate::table::common::IsAllowedId;
use crate::table::key_reader::KeyEntry;

use super::common::{HEADER_SIZE, MAGIC, ROW_OFFSET_SIZE};
use super::key_reader::KeyReader;
use super::reader_source_provider::SourceProvider;
use std::io::{self, Read};
use std::marker::PhantomData;

pub struct Decoder<T: IsAllowedId> {
    rows: u64,
    provider: SourceProvider,
    phantom: PhantomData<T>,
}

impl<T: IsAllowedId> Decoder<T> {
    pub fn new(provider: SourceProvider) -> Result<Self, std::io::Error> {
        let mut reader = provider.create_reader()?;
        let mut buffer = [0u8; HEADER_SIZE];
        reader.read_exact(&mut buffer)?;
        let magic: u64 = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
        let rows: u64 = u64::from_le_bytes(buffer[8..16].try_into().unwrap());
        if magic != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid magic number",
            ));
        }
        Ok(Decoder {
            rows,
            provider,
            phantom: PhantomData,
        })
    }

    pub fn query(&mut self, values: &[T]) -> io::Result<KeyReader<T>> {
        let row_position: Vec<KeyEntry<T>> = values
            .iter()
            .map(|&id| {
                let pos = id.to_u64() % self.get_rows();
                let row_offset = (HEADER_SIZE as u64) + ((ROW_OFFSET_SIZE as u64) * pos);
                KeyEntry { id, row_offset }
            })
            .collect();

        let reader = self.provider.create_reader()?;
        Ok(KeyReader::new(row_position, reader))
    }

    pub fn get_rows(&self) -> u64 {
        self.rows
    }
}

#[cfg(test)]

mod tests {
    use crate::table::reader_source_provider::MemoryCreator;

    use super::super::super::temp::file::TempFile;
    use super::super::common::*;
    use super::super::reader_source_provider::FileCreator;
    use super::*;
    use std::fs::File;
    use std::io::Write;

    fn create_header(rows: u64) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&MAGIC.to_le_bytes());
        data.extend_from_slice(&rows.to_le_bytes());
        data.extend(std::iter::repeat(0u8).take(16));
        data
    }
    fn row_offset_to_bytes(vec: &mut Vec<u8>, offset: u64, size: u32) {
        vec.extend_from_slice(&offset.to_le_bytes());
        vec.extend_from_slice(&size.to_le_bytes());
    }

    #[test]
    fn test_read_decoder_file() {
        let temp_file_guard = TempFile::new();
        let file_path = temp_file_guard.path();

        {
            let mut file = File::options().write(true).open(file_path).unwrap();
            let rows: u64 = 10;
            file.write_all(&create_header(rows)).unwrap();
        }

        let path_str = file_path.to_str().unwrap().to_string();

        let file_creator = FileCreator::new(path_str);
        let decoder = Decoder::<u32>::new(SourceProvider::File(file_creator))
            .expect("error creating decoder");
        assert_eq!(decoder.get_rows(), 10);
    }

    #[test]
    fn test_read_decoder_memory() {
        let data = create_header(10);
        let memory_creator = MemoryCreator::new(data);
        let provider = SourceProvider::Memory(memory_creator);
        let decoder = Decoder::<u32>::new(provider).expect("no decoder created");
        assert_eq!(decoder.get_rows(), 10);
    }

    fn offset_to_bytes<T: IsAllowedId>(offset: &OffsetHeader<T>) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.extend_from_slice(&offset.offset.to_le_bytes());
        vec.extend_from_slice(&offset.id.get_le_bytes());
        vec.extend_from_slice(&offset.size.to_le_bytes());
        vec
    }

    #[test]
    fn test_decoder_key_reader() {
        let mut data = create_header(1);
        let payload = b"Hello, Rust!";
        let offset = (HEADER_SIZE + ROW_OFFSET_SIZE) as u64;
        // we have the offset of the header
        // and we have how many elements is there
        row_offset_to_bytes(&mut data, offset, 1);

        let header = OffsetHeader {
            id: 1u32,
            offset: offset + 16,
            size: payload.len() as u32,
        };
        data.extend_from_slice(&offset_to_bytes(&header));
        data.extend_from_slice(payload);

        let memory_creator = MemoryCreator::new(data);
        let provider = SourceProvider::Memory(memory_creator);
        let mut decoder = Decoder::new(provider).expect("no decoder created");

        let ids: Vec<u32> = vec![1];
        let mut reader = decoder.query(&ids).expect("error getting key reader");
        let mut next_reader = reader
            .next_reader()
            .unwrap()
            .expect("error getting next reader");
        let mut buf = Vec::new();
        next_reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, payload);
    }
}
