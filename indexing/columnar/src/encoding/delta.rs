use std::io::{self, Write};
use std::sync::Mutex;

use crate::encoding::streaming::StreamingEncoder;
/// Delta encoding for monotonic integers.
pub struct DeltaStreamEncoder {
    prev: std::sync::Mutex<Option<i64>>,
}

impl Default for DeltaStreamEncoder {
    fn default() -> Self {
        Self {
            prev: Mutex::new(None),
        }
    }
}

impl DeltaStreamEncoder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StreamingEncoder<i64> for DeltaStreamEncoder {
    fn begin_stream(&self, _writer: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }

    fn encode_value(&self, v: &i64, _: usize, writer: &mut dyn Write) -> io::Result<()> {
        let mut guard = self.prev.lock().unwrap();
        let delta = match *guard {
            None => *v,
            Some(prev) => *v - prev,
        };
        writer.write_all(&delta.to_le_bytes())?;
        *guard = Some(*v);
        Ok(())
    }

    fn end_stream(&self, _writer: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }
}
