use crate::encoding::{StreamingEncoder, strings::doc_writer::DocWriter};
use fastbloom::BloomFilter;
use std::cell::RefCell;
use std::io;
use xxhash_rust::xxh3;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

const HEADER_SIZE: usize = 32;
const DOC_STREAM_MAGIC: &[u8; 6] = b"DOCST1";
const SIZE_DOC_OFFSET: usize = core::mem::size_of::<DocOffset>();

#[derive(Debug, Clone, Copy)]
struct DocStreamHeader {
    magic: [u8; 6],         // 6
    filter_offset: u32,     // 4
    filter_length: u32,     // 4
    doc_offset_offset: u32, // 4
    doc_offset_length: u32, // 4
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Immutable, IntoBytes, FromBytes, KnownLayout)]
struct DocOffset {
    offset: u64,  // 8
    id: u32,      // 4
    row: u32,     // 4
    size: u32,    // 4
    padding: u32, // 4
}

struct DocState {
    doc_offsets: Vec<DocOffset>,
    filter: BloomFilter,
}

pub struct DocStreamWriter {
    state: RefCell<DocState>,
    doc_writer: DocWriter,
}

impl Default for DocStreamWriter {
    fn default() -> Self {
        let filter = BloomFilter::with_num_bits(1 << 20).expected_items(2 << 20);
        Self {
            state: RefCell::new(DocState {
                filter,
                doc_offsets: vec![],
            }),
            doc_writer: DocWriter,
        }
    }
}

impl StreamingEncoder<String> for DocStreamWriter {
    fn begin_stream(&self, writer: &mut dyn std::io::Write) -> std::io::Result<()> {
        Ok(())
    }

    fn encode_value(
        &self,
        v: &String,
        _: usize,
        writer: &mut dyn std::io::Write,
    ) -> std::io::Result<()> {
        let tokens = process_string(v);
        self.doc_writer.write_dyn(&tokens, writer)?;
        let mut state = self.state.borrow_mut();
        tokens.iter().for_each(|val| {
            state.filter.insert(val);
        });
        Ok(())
    }

    fn end_stream(&self, writer: &mut dyn std::io::Write) -> std::io::Result<()> {
        // write the metadata here for the value.
        let state = self.state.borrow_mut();
        let offset_size = (state.doc_offsets.len() * SIZE_DOC_OFFSET) as u32;
        encode_doc_offset(writer, &state.doc_offsets)?;

        let filter_slice = state.filter.as_slice();
        let filter_len = filter_slice.len() as u32;
        encode_vec_64(writer, filter_slice)?;

        let mut header = [0u8; HEADER_SIZE];
        header[0..6].copy_from_slice(DOC_STREAM_MAGIC);
        header[6..10].copy_from_slice(filter_len.to_le_bytes().as_slice()); // store filter size
        header[14..18].copy_from_slice(offset_size.to_le_bytes().as_slice()); // store offset size

        writer.write_all(&header)?;
        Ok(())
    }
}

fn process_string(s: &str) -> Vec<u64> {
    s.split(" ").map(|s| xxh3::xxh3_64(s.as_bytes())).collect()
}

fn encode_vec_64(writer: &mut dyn std::io::Write, vec: &[u64]) -> io::Result<()> {
    #[cfg(target_endian = "little")]
    {
        use zerocopy::IntoBytes;
        writer.write_all(vec.as_bytes())?;
    }

    #[cfg(not(target_endian = "little"))]
    {
        for v in vec {
            writer.write_all(&v.to_le_bytes())?
        }
    }
    Ok(())
}

fn encode_doc_offsets_m(writer: &mut dyn std::io::Write, offsets: &[DocOffset]) -> io::Result<()> {
    let mut buffer = [0u8; SIZE_DOC_OFFSET];
    for offset in offsets {
        buffer[0..8].copy_from_slice(&offset.offset.to_le_bytes());
        buffer[8..12].copy_from_slice(&offset.id.to_le_bytes());
        buffer[12..16].copy_from_slice(&offset.row.to_le_bytes());
        buffer[16..20].copy_from_slice(&offset.size.to_le_bytes());
        writer.write_all(&buffer)?;
    }
    Ok(())
}

fn encode_doc_offset(writer: &mut dyn std::io::Write, offsets: &[DocOffset]) -> io::Result<()> {
    #[cfg(target_endian = "little")]
    {
        use zerocopy::IntoBytes;
        writer.write_all(offsets.as_bytes())?;
    }

    #[cfg(not(target_endian = "little"))]
    {
        encode_doc_offsets_m(writer, offsets)?;
    }
    Ok(())
}

fn decode_doc_offset(buffer: &[u8]) -> io::Result<Vec<DocOffset>> {
    if !buffer.len().is_multiple_of(SIZE_DOC_OFFSET) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "input byte size is not a multiple of DocOffset size",
        ));
    }

    #[cfg(target_endian = "little")]
    {
        use zerocopy::FromBytes;
        let result: Vec<DocOffset> = buffer
            .chunks(SIZE_DOC_OFFSET)
            .map(|b| match DocOffset::ref_from_bytes(b) {
                Ok(offset) => Ok(*offset),
                Err(e) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("cannot convert type {:?}", e),
                )),
            })
            .collect::<io::Result<_>>()?;
        Ok(result)
    }

    #[cfg(not(target_endian = "little"))]
    {
        let mut offsets = vec![];
        for i in (0..buffer.len()).step_by(SIZE_DOC_OFFSET) {
            let offset = DocOffset {
                offset: u64::from_le_bytes(buffer[i..i + 8].try_into().unwrap()),
                id: u32::from_le_bytes(buffer[i + 8..i + 12].try_into().unwrap()),
                row: u32::from_le_bytes(buffer[i + 12..i + 16].try_into().unwrap()),
                size: u32::from_le_bytes(buffer[i + 16..i + 20].try_into().unwrap()),
            };
            offsets.push(offset);
        }
        Ok(offsets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endian_encoding() {
        let doc_offsets = vec![
            DocOffset {
                id: 1,
                offset: 2,
                row: 3,
                size: 4,
                padding: 0,
            },
            DocOffset {
                id: 5,
                offset: 6,
                row: 7,
                size: 8,
                padding: 0,
            },
        ];

        let mut cursor1 = io::Cursor::new(Vec::new());
        encode_doc_offset(&mut cursor1, &doc_offsets).expect("err encoding value");

        let mut cursor2 = io::Cursor::new(Vec::new());
        encode_doc_offsets_m(&mut cursor2, &doc_offsets).expect("err encoding v2");

        assert_eq!(cursor1.get_ref(), cursor2.get_ref());
    }

    #[test]
    fn test_encode_decode_values() {
        let doc_offsets = vec![
            DocOffset {
                id: 1,
                offset: 2,
                row: 3,
                size: 4,
                padding: 0,
            },
            DocOffset {
                id: 5,
                offset: 6,
                row: 7,
                size: 8,
                padding: 0,
            },
        ];
        let mut vec = Vec::<u8>::new();
        encode_doc_offset(&mut vec, &doc_offsets).expect("err encoding value");

        let decoded = decode_doc_offset(&vec)
            .expect("err decoding value")
            .to_vec();
        assert_eq!(decoded.len(), doc_offsets.len());
        for i in 0..decoded.len() {
            assert_eq!(decoded[i].offset, doc_offsets[i].offset);
            assert_eq!(decoded[i].id, doc_offsets[i].id);
            assert_eq!(decoded[i].row, doc_offsets[i].row);
            assert_eq!(decoded[i].size, doc_offsets[i].size);
        }
    }
}
