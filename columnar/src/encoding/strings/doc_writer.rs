use crate::encoding::bitpack::v1::writer::encode_values;
use crate::encoding::strings::common::DOC_HEADER_SIZE;
use crate::encoding::strings::common::DOC_MAGIC;
use crate::encoding::strings::common::DOC_VERSION;
use std::collections::HashMap;
use std::io;
use std::io::Write;

pub struct DocWriter;

impl Default for DocWriter {
    fn default() -> Self {
        DocWriter
    }
}

impl DocWriter {
    pub fn write<W>(&mut self, tokens: &[u64], writer: &mut W) -> io::Result<usize>
    where
        W: Write,
    {
        if tokens.is_empty() {
            return Ok(0);
        }

        // Step 1: Collect positions
        let mut table = HashMap::<u64, Vec<u32>>::new();
        for (pos, token) in tokens.iter().enumerate() {
            table.entry(*token).or_default().push(pos as u32);
        }

        // Step 2: Encode position lists
        let mut encoded_entries: Vec<(u64, Vec<u8>)> = table
            .into_iter()
            .map(|(key, positions)| {
                let (width, buffer) = encode_values(positions.as_slice()).unwrap();
                let mut vec = Vec::new();
                vec.extend_from_slice(&(buffer.len() as u32).to_le_bytes()); // attach the length
                vec.extend_from_slice(&[width]); // attach the width
                vec.extend_from_slice(&buffer); // attach the values
                println!(
                    "Encoded key {} width {} with {} positions into {} bytes",
                    key,
                    width,
                    positions.len(),
                    vec.len()
                );
                (key, vec)
            })
            .collect();

        encoded_entries.sort_unstable_by_key(|&(key, _)| key);
        let entry_count = encoded_entries.len();

        // Step 3: Compute sizes
        let entries_size: usize = entry_count * 16; // 2 * u64 per entry
        let data_size: usize = encoded_entries.iter().map(|(_, data)| data.len()).sum();
        let mut header = [0u8; DOC_HEADER_SIZE];
        header[0..6].copy_from_slice(DOC_MAGIC);
        header[6] = DOC_VERSION;
        header[7..11].copy_from_slice(&(data_size as u32).to_le_bytes()); // data size
        header[11..15].copy_from_slice(&(entry_count as u32).to_le_bytes()); // entry count
        writer.write_all(&header)?;

        let mut current_offset = 0u64; // offset relative to after entry_count  
        for &(key, ref data) in &encoded_entries {
            writer.write_all(&key.to_le_bytes())?;
            writer.write_all(&current_offset.to_le_bytes())?;
            current_offset += data.len() as u64;
        }

        for (_, data) in encoded_entries {
            writer.write_all(&data)?;
        }

        Ok(data_size + entries_size + DOC_HEADER_SIZE)
    }
}
