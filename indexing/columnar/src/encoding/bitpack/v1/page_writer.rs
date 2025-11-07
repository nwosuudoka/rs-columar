use crate::buffers::smart_pool::{SmartBufferPool, SmartPage};
use crate::encoding::bitpack::v1::common::BitEncodable;
use crate::encoding::bitpack::v1::common::{
    PAGE_DEFAULT_SIZE, PAGE_HEADER_SIZE, PAGE_MAGIC_BITPACK, PAGE_VERSION,
};
use crate::encoding::bitpack::v1::writer::BitWriterRef;
use std::io;
use std::iter::Peekable;

pub struct PageEncoder<I, T>
where
    I: Iterator<Item = T>,
    T: BitEncodable,
{
    pool: SmartBufferPool,
    input: Peekable<I>,
    width: u8,
    values_per_page: usize,
    page_size: usize,
}

impl<I, T> PageEncoder<I, T>
where
    I: Iterator<Item = T>,
    T: BitEncodable,
{
    pub fn new(pool: SmartBufferPool, input: I, width: u8, page_size: usize) -> Self {
        let values_per_page = if width > 0 {
            page_size.saturating_sub(PAGE_HEADER_SIZE) * 8 / (width as usize)
        } else {
            PAGE_DEFAULT_SIZE
        };
        Self {
            pool,
            input: input.peekable(),
            width,
            values_per_page,
            page_size,
        }
    }
}

impl<I, T> Iterator for PageEncoder<I, T>
where
    I: Iterator<Item = T>,
    T: BitEncodable,
{
    type Item = io::Result<SmartPage>;

    fn next(&mut self) -> Option<Self::Item> {
        self.input.peek()?;

        let mut buffer = self.pool.get(self.page_size);
        buffer.clear();
        buffer.resize_uninit(PAGE_HEADER_SIZE);

        let mut writer = BitWriterRef::new(buffer.vec_mut(), self.width);

        let mut count = 0;
        let mut min = T::MAX;
        let mut max = T::MIN;

        while count < self.values_per_page {
            match self.input.next() {
                Some(v) => {
                    // writer fails return error
                    if let Err(e) = writer.write_value(v) {
                        return Some(Err(e));
                    }
                    min = min.min(v);
                    max = max.max(v);
                    count += 1;
                }
                None => break,
            }
        }

        if let Err(e) = writer.flush() {
            return Some(Err(e));
        }

        drop(writer);

        let mut header = [0u8; PAGE_HEADER_SIZE];
        header[..6].copy_from_slice(PAGE_MAGIC_BITPACK);
        header[6] = PAGE_VERSION;
        header[7] = (T::BITS / 8) as u8;
        header[8] = self.width;
        header[9..17].copy_from_slice(&(count as u64).to_le_bytes());

        let type_width = (T::BITS / 8) as usize;
        let start = 17;
        let end = 17 + type_width;
        header[start..end].copy_from_slice(&min.to_le_bytes());

        let start = end;
        let end = start + type_width;
        header[start..end].copy_from_slice(&max.to_le_bytes());

        let start = end;
        let end = start + 8;
        let data_bytes = (buffer.len() - PAGE_HEADER_SIZE) as u64;
        header[start..end].copy_from_slice(&data_bytes.to_le_bytes());

        // page.buffer.as_mut_slice()[..PAGE_DEFAULT_SIZE].copy_from_slice(&header);
        buffer.as_mut_slice()[..PAGE_HEADER_SIZE].copy_from_slice(&header);
        Some(Ok(buffer))
    }
}
