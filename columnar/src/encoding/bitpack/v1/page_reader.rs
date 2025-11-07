use crate::{
    buffers::smart_pool::{SmartBufferPool, SmartPage},
    encoding::bitpack::v1::{
        common::{BitEncodable, PAGE_HEADER_SIZE, PAGE_MAGIC_BITPACK, PAGE_VERSION},
        reader::BitStream,
    },
};
use std::io::{self, Cursor, Read};

pub struct PageHeader<T: BitEncodable> {
    pub min: T,
    pub max: T,
    pub count: usize,
    pub bit_width: u8,
    pub data_bytes: u64,
}

impl<T: BitEncodable> PageHeader<T> {
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut header_buf = [0u8; PAGE_HEADER_SIZE];
        reader.read_exact(&mut header_buf)?;

        if &header_buf[0..6] != PAGE_MAGIC_BITPACK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid page magic {:?} != {:?}",
                    PAGE_MAGIC_BITPACK,
                    &header_buf[0..6]
                ),
            ));
        }

        if header_buf[6] != PAGE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported page version {}", header_buf[6]),
            ));
        }

        let type_width = header_buf[7] as usize;
        if (type_width * 8) != T::BITS as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "type width mismatch: expected {}, found {}",
                    T::BITS,
                    type_width * 8
                ),
            ));
        }

        let bit_width = header_buf[8];
        let count = u64::from_le_bytes(header_buf[9..17].try_into().unwrap()) as usize;

        let type_width = (T::BITS / 8) as usize;

        let start: usize = 17;
        let end = start + type_width;
        let min = T::from_le_bytes(&header_buf[start..end]);

        let start = end;
        let end = start + type_width;
        let max = T::from_le_bytes(&header_buf[start..end]);

        let start = end;
        let end = start + 8;

        let data_bytes = u64::from_le_bytes(header_buf[start..end].try_into().unwrap());
        Ok(Self {
            min,
            max,
            count,
            bit_width,
            data_bytes,
        })
    }
}

pub struct PageDecoder<R: Read, T: BitEncodable> {
    pool: SmartBufferPool,
    source_reader: R,
    current_stream: Option<BitStream<Cursor<SmartPage>, T>>,
}

impl<R: Read, T: BitEncodable> PageDecoder<R, T> {
    pub fn new(pool: SmartBufferPool, source_reader: R) -> Self {
        Self {
            pool,
            source_reader,
            current_stream: None,
        }
    }
}

impl<R: Read, T: BitEncodable> Iterator for PageDecoder<R, T> {
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut stream) = self.current_stream {
                match stream.next() {
                    Some(item) => return Some(item),
                    None => {
                        self.current_stream = None;
                    }
                }
            }

            match PageHeader::<T>::read_from(&mut self.source_reader) {
                Ok(header) => {
                    let mut buffer = self.pool.get(header.data_bytes as usize);
                    buffer.resize_uninit(header.data_bytes as usize);

                    if let Err(e) = self.source_reader.read_exact(buffer.as_mut_slice()) {
                        return Some(Err(e));
                    }
                    let cursor = io::Cursor::new(buffer);
                    let bit_stream = BitStream::with_count(cursor, header.bit_width, header.count);
                    self.current_stream = Some(bit_stream);
                    continue;
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        return None;
                    } else {
                        return Some(Err(e));
                    }
                }
            }
        }
    }
}

/// An iterator that decodes values from a stream of bit-packed pages,
/// using a BufferPool and supporting predicate-based page skipping.
pub struct PooledPageDecoder<R, T, F>
where
    R: Read,
    T: BitEncodable,
    F: FnMut(&PageHeader<T>) -> bool,
{
    pool: SmartBufferPool,
    source_reader: R,
    current_stream: Option<BitStream<Cursor<SmartPage>, T>>,
    predicate: F,
}

impl<R, T, F> PooledPageDecoder<R, T, F>
where
    R: Read,
    T: BitEncodable,
    F: FnMut(&PageHeader<T>) -> bool,
{
    /// Creates a new decoder with a predicate for filtering pages.
    ///
    /// The predicate is a closure that receives a reference to a `PageHeader`
    /// and returns `true` to decode the page or `false` to skip it.
    pub fn with_predicate(pool: SmartBufferPool, reader: R, predicate: F) -> Self {
        Self {
            pool,
            source_reader: reader,
            current_stream: None,
            predicate,
        }
    }
}

// A second constructor for convenience when no filtering is needed.
impl<R, T> PooledPageDecoder<R, T, fn(&PageHeader<T>) -> bool>
where
    R: Read,
    T: BitEncodable,
{
    /// Creates a new decoder that processes all pages without filtering.
    pub fn new(pool: SmartBufferPool, reader: R) -> Self {
        Self::with_predicate(pool, reader, |_| true)
    }
}

impl<R, T, F> Iterator for PooledPageDecoder<R, T, F>
where
    R: Read,
    T: BitEncodable,
    F: FnMut(&PageHeader<T>) -> bool,
{
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have an active page stream, get the next value from it.
            if let Some(ref mut stream) = self.current_stream {
                match stream.next() {
                    Some(item) => return Some(item),
                    None => self.current_stream = None, // Page is exhausted.
                }
            }

            // We need to load a new page.
            match PageHeader::<T>::read_from(&mut self.source_reader) {
                Ok(header) => {
                    // *** PREDICATE LOGIC IS HERE ***
                    if (self.predicate)(&header) {
                        // KEEP THE PAGE: Load its data into a buffer and decode.
                        let mut buffer = self.pool.get(header.data_bytes as usize);
                        buffer.resize_uninit(header.data_bytes as usize);
                        if let Err(e) = self.source_reader.read_exact(buffer.as_mut_slice()) {
                            return Some(Err(e));
                        }

                        let cursor = Cursor::new(buffer);
                        let stream = BitStream::with_count(cursor, header.bit_width, header.count);
                        self.current_stream = Some(stream);

                        // Loop again to pull the first value from the new stream.
                        continue;
                    } else {
                        // SKIP THE PAGE: Consume and discard its data section without buffering.
                        let mut limited_reader =
                            self.source_reader.by_ref().take(header.data_bytes);
                        if let Err(e) = io::copy(&mut limited_reader, &mut io::sink()) {
                            return Some(Err(e));
                        }
                        // Loop again to find the next valid page header.
                        continue;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None, // Clean EOF.
                Err(e) => return Some(Err(e)),                                     // Fatal error.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffers::smart_pool::SmartBufferPool;
    use crate::buffers::smart_pool::SmartPage;
    use crate::encoding::bitpack::v1::page_reader::{PageHeader, PooledPageDecoder};
    use crate::encoding::bitpack::v1::page_writer::PageEncoder;
    use std::io::{self, Cursor};

    /// A comprehensive roundtrip test for the encoder and decoder.
    ///
    /// This test verifies:
    /// 1.  Data is encoded into multiple pages correctly.
    /// 2.  The decoder can read the stream of pages.
    /// 3.  The predicate correctly filters out entire pages based on header stats.
    /// 4.  The final decoded data matches the expected (filtered) original data.
    #[test]
    fn test_full_roundtrip_with_predicate_filtering() -> io::Result<()> {
        // --- 1. SETUP ---
        let pool = SmartBufferPool::new(4);
        let bit_width = 10; // Use a non-byte-aligned width to test bitpacking.
        let page_size = 128; // A small page size to ensure we create multiple pages.

        let values_per_page = 51 as usize;
        // Create test data designed to be filtered. We will have 3 pages:
        // - Page 1: Values in the 100s
        // - Page 2: Values in the 900s (this is the page we want to keep)
        // - Page 3: Values in the 400s

        // Generate data based on the new, correct page capacity.
        let source_data_p1 = 100u32..(100 + values_per_page as u32); // Page 1: [100, 150]
        let source_data_p2 = 900u32..(900 + values_per_page as u32); // Page 2: [900, 950] (Target)
        let source_data_p3 = 400u32..(400 + values_per_page as u32); // Page 3: [400, 450]

        let source_data = source_data_p1
            .clone()
            .chain(source_data_p2.clone())
            .chain(source_data_p3.clone());

        // --- 2. ENCODE ---
        let encoder = PageEncoder::new(pool.clone(), source_data, bit_width, page_size);

        // Collect all encoded pages from the iterator.
        let encoded_pages: Vec<SmartPage> = encoder.collect::<io::Result<Vec<_>>>()?;
        let num_pages = encoded_pages.len();
        assert_eq!(num_pages, 3, "Expected 3 pages to be created");

        // Simulate a single contiguous file/stream by concatenating the page buffers.
        let mut encoded_stream_bytes = Vec::new();
        for page_buffer in encoded_pages {
            encoded_stream_bytes.extend_from_slice(page_buffer.as_slice());
        }
        assert!(!encoded_stream_bytes.is_empty());

        // --- 3. DECODE with PREDICATE ---

        // The predicate will inspect the header of each page and only accept
        // the one whose data falls within the [900, 1000] range.
        let predicate = |header: &PageHeader<u32>| {
            println!(
                "Predicate checking page: min={}, max={}. Keeping? {}",
                header.min,
                header.max,
                header.min >= 900
            );
            // We only care about the page with the 900-series numbers.
            header.min >= 900 && header.max < 1000
        };

        let stream_reader = Cursor::new(encoded_stream_bytes);
        let decoder = PooledPageDecoder::with_predicate(pool.clone(), stream_reader, predicate);

        // Collect the results. The decoder should transparently skip the first and third pages.
        let decoded_results: Vec<u32> = decoder.collect::<io::Result<Vec<_>>>()?;

        // --- 4. VERIFY ---

        // The expected result is ONLY the data from the second page.
        // let expected_results: Vec<u32> = (900..(900 + values_per_page)).collect();
        let expected_results: Vec<u32> = source_data_p2.collect();

        assert_eq!(
            decoded_results.len(),
            values_per_page,
            "Expected {} values after filtering, got {}",
            values_per_page,
            decoded_results.len()
        );
        assert_eq!(
            decoded_results, expected_results,
            "The decoded data did not match the filtered source data."
        );

        Ok(())
    }

    #[test]
    fn test_empty_input_roundtrip() -> io::Result<()> {
        let pool = SmartBufferPool::new(2 << 20);
        let source_data: Vec<u32> = vec![];

        // Encode
        let encoder = PageEncoder::new(pool.clone(), source_data.clone().into_iter(), 8, 1024);
        let encoded_pages: Vec<SmartPage> = encoder.collect::<io::Result<Vec<_>>>()?;
        assert!(
            encoded_pages.is_empty(),
            "Encoding empty data should produce no pages"
        );

        let encoded_stream_bytes: Vec<u8> = vec![];

        // Decode
        let stream_reader = Cursor::new(encoded_stream_bytes);
        let decoder = PooledPageDecoder::new(pool.clone(), stream_reader);
        let decoded_results: Vec<u32> = decoder.collect::<io::Result<Vec<_>>>()?;

        // Verify
        assert!(
            decoded_results.is_empty(),
            "Decoding an empty stream should produce no values"
        );

        Ok(())
    }
}
