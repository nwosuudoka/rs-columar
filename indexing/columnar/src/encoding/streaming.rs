use std::io::{self, Read, Write};
/// Trait for streaming encoders: stateful, incremental encoders that
/// can write data as it arrives.
// pub trait StreamingEncoder<T>: Send + Sync + 'static {
pub trait StreamingEncoder<T>: Send + 'static {
    fn begin_stream(&self, writer: &mut dyn Write) -> io::Result<()>;
    fn encode_value(&self, v: &T, row_pos: usize, writer: &mut dyn Write) -> io::Result<()>;
    fn end_stream(&self, writer: &mut dyn Write) -> io::Result<()>;
}

pub trait StreamingDecoder<T>: Send {
    fn begin_stream(&mut self, reader: &mut dyn Read) -> io::Result<()>;
    fn decode_next(&mut self, reader: &mut dyn Read) -> io::Result<Option<T>>;
    fn end_stream(&mut self, reader: &mut dyn Read) -> io::Result<()>;
}
