use crate::buffers::smart_pool::{SmartBufferPool, SmartPage};
use crate::encoding::StreamingEncoder;
use crate::encoding::bitpack::v1::common::{BitEncodable, PAGE_DEFAULT_SIZE, bit_width_from_value};
use crate::encoding::bitpack::v1::page_writer::PageEncoder;
use crate::encoding::iters::num::NumReadIter;
use std::fs;
use std::io::{self, Seek, Write};
use std::sync::Mutex;

const BUFFER_SIZE: usize = 1 << 20;

pub struct BitpackStreamWriter<T: BitEncodable> {
    state: Mutex<Option<BitpackState<T>>>,
    pool: SmartBufferPool,
    bit_size: usize,
}

struct BitpackState<T: BitEncodable> {
    file: fs::File,
    buffer: SmartPage,
    min: T,
    max: T,
    count: u64,
}

impl<T: BitEncodable> BitpackStreamWriter<T> {
    pub fn new(pool: SmartBufferPool) -> Self {
        let file = tempfile::tempfile().expect("failed to create a temp file");
        let mut buffer = pool.get(BUFFER_SIZE);
        buffer.clear();
        buffer.resize_uninit(BUFFER_SIZE);

        let state = Mutex::new(Some(BitpackState {
            buffer,
            file,
            max: T::MIN,
            min: T::MAX,
            count: 0,
        }));
        let bit_size = core::mem::size_of::<T>();
        Self {
            state,
            bit_size,
            pool,
        }
    }

    fn flush_buffer(&self, state: &mut BitpackState<T>) -> io::Result<()> {
        if state.buffer.len() > 0 {
            state.file.write_all(state.buffer.as_slice())?;
            state.buffer.clear();
        }
        Ok(())
    }
}

impl<T: BitEncodable> Default for BitpackStreamWriter<T> {
    fn default() -> Self {
        let file = tempfile::tempfile().expect("failed to create a temp file");
        let pool = SmartBufferPool::new(4 * 1024);
        let mut buffer = pool.get(BUFFER_SIZE);
        buffer.clear();
        buffer.resize_uninit(BUFFER_SIZE);

        let state = Mutex::new(Some(BitpackState {
            buffer,
            file,
            max: T::MIN,
            min: T::MAX,
            count: 0,
        }));
        let bit_size = core::mem::size_of::<T>();
        Self {
            state,
            bit_size,
            pool,
        }
    }
}

impl<T> StreamingEncoder<T> for BitpackStreamWriter<T>
where
    T: BitEncodable,
    T: Sync + Send + 'static,
{
    fn begin_stream(&self, _: &mut dyn std::io::Write) -> std::io::Result<()> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.as_mut().unwrap();
        state.file.set_len(0).ok(); // truncate
        state.min = T::MAX;
        state.max = T::MIN;
        state.count = 0;
        state.buffer.clear();
        Ok(())
    }

    fn encode_value(&self, v: &T, _: &mut dyn std::io::Write) -> std::io::Result<()> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.as_mut().unwrap();
        state.min = state.min.min(*v);
        state.max = state.max.max(*v);
        state.count += 1;
        // state.buffer.extend_from_slice(&v.to_le_bytes());
        state
            .buffer
            .append_slice(&v.to_le_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Buffer capacity exceeded"))?;
        if state.buffer.len() >= BUFFER_SIZE {
            self.flush_buffer(state)?;
        }
        Ok(())
    }

    fn end_stream(&self, writer: &mut dyn std::io::Write) -> std::io::Result<()> {
        let mut guard = self.state.lock().unwrap();
        let state = guard.as_mut().unwrap();

        // Flush remaining buffer
        self.flush_buffer(state)?;
        state.file.flush()?;

        // Handle empty case
        if state.count == 0 {
            return Ok(());
        }

        // Rewind temp file
        state.file.seek(std::io::SeekFrom::Start(0))?;

        // Determine bit width
        // NOTE: Decide if you're packing raw values or normalized (v - min)
        let width = bit_width_from_value::<T>(state.max); // or (state.max - state.min)
        let reader = io::BufReader::with_capacity(BUFFER_SIZE, &state.file);
        let num_reader = NumReadIter::<_, T>::new(reader).flatten();

        let page_encoder =
            PageEncoder::new(self.pool.clone(), num_reader, width, PAGE_DEFAULT_SIZE);
        for page_result in page_encoder {
            let page = page_result?;
            writer.write_all(&page.buf)?;
        }
        writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::bitpack::v1::page_reader::PageDecoder;
    use std::io::Cursor;

    #[test]
    fn test_bitpack_stream() {
        let pool = SmartBufferPool::new(4 * 1024);
        let writer = BitpackStreamWriter::<u8>::new(pool.clone());
        let mut cursor = Cursor::new(Vec::new());
        writer.begin_stream(&mut cursor).unwrap();
        writer.encode_value(&1, &mut cursor).unwrap();
        writer.encode_value(&2, &mut cursor).unwrap();
        writer.encode_value(&3, &mut cursor).unwrap();
        writer.encode_value(&4, &mut cursor).unwrap();
        writer.end_stream(&mut cursor).unwrap();

        let mut decoder = PageDecoder::<_, u8>::new(pool.clone(), Cursor::new(cursor.into_inner()));
        assert_eq!(decoder.next().unwrap().unwrap(), 1);
        assert_eq!(decoder.next().unwrap().unwrap(), 2);
        assert_eq!(decoder.next().unwrap().unwrap(), 3);
        assert_eq!(decoder.next().unwrap().unwrap(), 4);
    }
}
