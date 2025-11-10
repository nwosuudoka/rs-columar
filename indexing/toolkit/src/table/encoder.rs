use std::io::{self, BufWriter, ErrorKind, Write};
use std::path::PathBuf;

use crate::table::common::{HEADER_SIZE, IsAllowedId, MAGIC, OffsetHeader, ROW_OFFSET_SIZE};
const PAGE_SIZE: usize = 512;

pub struct Encoder<T: IsAllowedId> {
    writer: BufWriter<std::fs::File>,
    offset: u64,
    vec: Vec<OffsetHeader<T>>,
}

impl<T: IsAllowedId> Encoder<T> {
    pub fn new(tmp_dir: PathBuf) -> io::Result<Self> {
        let tmp_file_path = tmp_dir.join("tmp_file.bin");
        let file = std::fs::File::create(tmp_file_path).unwrap();
        let writer = BufWriter::new(file);
        Ok(Self {
            writer,
            offset: 0,
            vec: Vec::new(),
        })
    }

    pub fn write(&mut self, id: T, data: &[u8]) -> io::Result<u64> {
        let offset = self.offset;
        self.offset += data.len() as u64;
        self.writer.write_all(data)?;
        self.vec.push(OffsetHeader {
            id,
            offset,
            size: data.len() as u32,
        });
        Ok(offset)
    }

    // we do not care for performance here so we can just use a dynamic reader.
    pub fn write_from_reader(&mut self, id: T, reader: &mut dyn io::Read) -> io::Result<u64> {
        let offset = self.offset;
        match io::copy(reader, &mut self.writer) {
            Ok(size) => {
                self.offset += size;
                self.vec.push(OffsetHeader {
                    id: id.into(),
                    offset,
                    size: size as u32,
                });
                Ok(offset)
            }
            Err(e) => Err(e),
        }
    }

    pub fn write_multi_key(&mut self, ids: &[T], reader: &mut dyn io::Read) -> io::Result<u64> {
        let offset = self.offset;
        match io::copy(reader, &mut self.writer) {
            Ok(size) => {
                self.offset += size;
                for id in ids {
                    self.vec.push(OffsetHeader {
                        id: *id,
                        offset,
                        size: size as u32,
                    });
                }
                Ok(offset)
            }
            Err(e) => Err(e),
        }
    }

    pub fn export<W: io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        if self.vec.is_empty() {
            return Err(io::Error::new(ErrorKind::InvalidData, "no rows to write"));
        }

        let mut buffer_writer = BufWriter::new(w);

        let header_size = 8 + T::byte_size() + 4;
        let bucket_len = (self.vec.len() * header_size / PAGE_SIZE) + 1;
        let mut matrix = Vec::with_capacity(bucket_len);
        for _ in 0..bucket_len {
            matrix.push(Vec::new());
        }

        for row in &self.vec {
            // we want to explicitly move here
            let bucket = (row.id.to_u64() % (bucket_len as u64)) as usize;
            matrix[bucket].push(row);
        }
        // matrix.set_len(bucket_len);
        for row in &mut matrix {
            row.sort_by_key(|x| x.id.to_u64());
        }

        let num_buckets = matrix.len();

        let mut offsets = Vec::<(u64, u32)>::with_capacity(num_buckets);
        let mut current_offset = HEADER_SIZE + (ROW_OFFSET_SIZE * num_buckets);
        for row in &matrix {
            offsets.push((current_offset as u64, row.len() as u32));
            current_offset += row.len() + header_size;
        }

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&u64::to_le_bytes(MAGIC));
        buffer.extend_from_slice(&u64::to_le_bytes(num_buckets as u64));

        for (offset, size) in offsets {
            buffer.extend_from_slice(&u64::to_le_bytes(offset));
            buffer.extend_from_slice(&size.to_le_bytes());
        }

        // write the header and offsets here
        buffer_writer.write_all(&buffer)?;

        let mut data_buffer = vec![0u8; header_size];
        for row in &matrix {
            buffer.clear();
            for header in row {
                header.write_to_buffer(&mut data_buffer);
                buffer.extend_from_slice(&data_buffer);
            }
            buffer_writer.write_all(&buffer)?;
        }
        Ok(())
    }
}

#[cfg(test)]

mod test {
    use super::super::super::temp::dir::TempDir;
    use super::*;
    use std::env;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_encoder_write() {
        let temp_dir = TempDir::new().expect("error creating temp dir");
        let mut encoder = Encoder::new(temp_dir.path()).expect("error creating encoder");
        encoder
            .write(1_u32, b"Hello, Rust!")
            .expect("error writing byte values");
        let mut reader = io::Cursor::new(b"Hello, 2!");
        encoder
            .write_from_reader(2_u32, &mut reader)
            .expect("error writing from reader");
        encoder
            .write_multi_key(&[3_u32, 4_u32], &mut io::Cursor::new(b"Hello, 3,4!"))
            .expect("error writing multi key");

        let mut out_writer = io::Cursor::new(Vec::new());
        encoder
            .export(&mut out_writer)
            .expect("error exporting to cursor");
    }
}
