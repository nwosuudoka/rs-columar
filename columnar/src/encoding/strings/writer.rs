use crate::encoding::streaming::StreamingEncoder;
use std::io::{self, Write};
pub struct StringWriter;

impl<String> StreamingEncoder<String> for StringWriter {
    fn begin_stream(&self, writer: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }
    fn encode_value(&self, v: &String, writer: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }
    fn end_stream(&self, writer: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }
}
