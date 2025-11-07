use std::io::Write;

use crate::encoding::streaming::StreamingEncoder;

/// Writes each value as fixed-width binary (e.g., 8 bytes for u64).
pub struct FixedWidthStreamEncoder;

impl<T: Copy> StreamingEncoder<T> for FixedWidthStreamEncoder {
    fn begin_stream(&self, _writer: &mut dyn Write) -> std::io::Result<()> {
        Ok(())
    }
    fn encode_value(&self, v: &T, _: usize, writer: &mut dyn Write) -> std::io::Result<()> {
        let bytes = unsafe {
            std::slice::from_raw_parts((v as *const T) as *const u8, std::mem::size_of::<T>())
        };
        writer.write_all(bytes)
    }
    fn end_stream(&self, _writer: &mut dyn Write) -> std::io::Result<()> {
        Ok(())
    }
}
