use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Mutex;

pub struct SectionedSlice<'a, F>
where
    F: Read + Seek,
{
    slice_inner: &'a Mutex<F>,
    start_offset: u64,
    size: u64,
    current_pos: u64,
}

impl<'a, F> Read for SectionedSlice<'a, F>
where
    F: Read + Seek,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Before any I/O, we must lock the mutex to gain exclusive access to the file handle.
        // The lock is released automatically when `inner_guard` goes out of scope.
        let mut inner_guard = self
            .slice_inner
            .lock()
            .map_err(|_| io::Error::other("Mutex was poisoned"))?;

        let bytes_left = self.size.saturating_sub(self.current_pos);
        if bytes_left == 0 {
            return Ok(0); // End of slice.
        }

        let bytes_to_read = std::cmp::min(buf.len(), bytes_left as usize);
        let limited_buf = &mut buf[..bytes_to_read];

        // **Crucial Step**: Move the physical file cursor to the correct absolute position.
        let absolute_pos = self.start_offset + self.current_pos;
        inner_guard.seek(SeekFrom::Start(absolute_pos))?;

        let num_bytes_read = inner_guard.read(limited_buf)?;

        // advance our relative position
        self.current_pos += num_bytes_read as u64;
        Ok(num_bytes_read)
    }
}

impl<'a, F> Seek for SectionedSlice<'a, F>
where
    F: Read + Seek,
{
    /// Seeking on a `FileSlice` is efficient as it only updates the internal
    /// relative cursor (`current_pos`). The physical file cursor is only moved
    /// during a subsequent `read` operation.
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos_relative = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(p) => self.size as i64 + p,
            SeekFrom::Current(p) => self.current_pos as i64 + p,
        };

        if new_pos_relative < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start of slice",
            ));
        }

        let new_pos_relative = new_pos_relative as u64;

        // Note: Seeking beyond the end is allowed by the Seek trait, but reading
        // from there should yield EOF. Our `read` implementation already handles this.

        self.current_pos = new_pos_relative;
        Ok(self.current_pos)
    }
}

pub struct FileSliceColumn {
    pub id: u32,
    pub offset: u64,
    pub size: u64,
}

pub struct FileSlicer<F>
where
    F: Read + Seek,
{
    inner: Mutex<F>,
    sections: HashMap<u32, (u64, u64)>,
}

impl<F> FileSlicer<F>
where
    F: Read + Seek,
{
    pub fn new(inner: F, sections: Vec<FileSliceColumn>) -> Self {
        Self {
            inner: Mutex::new(inner),
            sections: sections
                .into_iter()
                .map(|col| (col.id, (col.offset, col.size)))
                .collect(),
        }
    }

    pub fn get_slice(&self, id: u32) -> Option<SectionedSlice<'_, F>> {
        let &(start_offset, size) = self.sections.get(&id)?;
        Some(SectionedSlice {
            slice_inner: &self.inner,
            start_offset,
            size,
            current_pos: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp::dir::{TempDir, tempdir};
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::path::PathBuf;

    /// Test fixture that creates a file with known, distinct sections.
    /// Content: "AAAAAAAAAABBBBBCCCCCDDDDD" (25 bytes total).
    fn create_test_file_and_slicer(name: &str) -> (PathBuf, TempDir, FileSlicer<File>) {
        let temp_dir = tempdir().unwrap();
        let filepath = temp_dir.path().join(name);
        let mut file = File::create(&filepath).unwrap();
        file.write_all(b"AAAAAAAAAABBBBBCCCCCDDDDD").unwrap();
        file.flush().unwrap();

        let file_to_slice = File::open(&filepath).unwrap();
        let sections = vec![
            (1, 0, 10),  // Section 'A'
            (2, 10, 5),  // Section 'B'
            (3, 15, 5),  // Section 'C'
            (99, 10, 0), // Zero-length section
        ]
        .into_iter()
        .map(|(id, offset, size)| FileSliceColumn { id, offset, size })
        .collect();
        let slicer = FileSlicer::new(file_to_slice, sections);
        (filepath, temp_dir, slicer)
    }

    #[test]
    fn test_slicer_get_slice() {
        let (path, _temp_dir, slicer) = create_test_file_and_slicer("get_slice.bin");

        // Get a valid slice
        let slice_a = slicer.get_slice(1);
        assert!(slice_a.is_some());
        let slice = slice_a.unwrap();
        assert_eq!(slice.start_offset, 0);
        assert_eq!(slice.size, 10);

        // Get another valid slice
        let slice_b = slicer.get_slice(2);
        assert!(slice_b.is_some());
        let slice = slice_b.unwrap();
        assert_eq!(slice.start_offset, 10);
        assert_eq!(slice.size, 5);

        // Get a non-existent slice
        let slice_none = slicer.get_slice(404);
        assert!(slice_none.is_none());

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_slice_read_full_content() {
        let (path, _temp_dir, slicer) = create_test_file_and_slicer("read_full.bin");
        let mut slice_b = slicer.get_slice(2).unwrap();

        let mut content = Vec::new();
        slice_b.read_to_end(&mut content).unwrap();

        assert_eq!(content, b"BBBBB");

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_slice_read_in_chunks() {
        let (path, _temp_dir, slicer) = create_test_file_and_slicer("read_chunks.bin");
        let mut slice_a = slicer.get_slice(1).unwrap();

        let mut buf = [0u8; 4];

        // Read first 4 bytes
        slice_a.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"AAAA");

        // Read next 4 bytes
        slice_a.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"AAAA");

        // Read last 2 bytes
        let mut last_buf = [0u8; 2];
        slice_a.read_exact(&mut last_buf).unwrap();
        assert_eq!(&last_buf, b"AA");

        // Reading again should yield EOF
        let bytes_read = slice_a.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_slice_seek_and_read() {
        let (path, temp_dir, slicer) = create_test_file_and_slicer("seek_read.bin");
        let mut slice_a = slicer.get_slice(1).unwrap();

        // Seek from start and read
        slice_a.seek(SeekFrom::Start(8)).unwrap();
        let mut buf = [0u8; 2];
        slice_a.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"AA");

        // Seek from end and read
        slice_a.seek(SeekFrom::End(-5)).unwrap();
        let mut buf2 = [0u8; 5];
        slice_a.read_exact(&mut buf2).unwrap();
        assert_eq!(&buf2, b"AAAAA");

        // Seek from current and read
        slice_a.seek(SeekFrom::Start(2)).unwrap(); // pos=2
        slice_a.seek(SeekFrom::Current(4)).unwrap(); // pos=6
        let mut buf3 = [0u8; 4];
        slice_a.read_exact(&mut buf3).unwrap();
        assert_eq!(&buf3, b"AAAA");

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_slice_reads_are_isolated() {
        let (path, _temp_dir, slicer) = create_test_file_and_slicer("isolated_reads.bin");
        let mut slice_a = slicer.get_slice(1).unwrap();
        let mut slice_c = slicer.get_slice(3).unwrap();

        let mut buf_a = [0u8; 4];
        slice_a.read_exact(&mut buf_a).unwrap();
        assert_eq!(&buf_a, b"AAAA");

        let mut buf_c = [0u8; 3];
        slice_c.read_exact(&mut buf_c).unwrap();
        assert_eq!(&buf_c, b"CCC");

        // Go back to A, its position should be unaffected by C's read
        let mut buf_a2 = [0u8; 6];
        slice_a.read_exact(&mut buf_a2).unwrap();
        assert_eq!(&buf_a2, b"AAAAAA");

        // Go back to C, its position should be unaffected by A's read
        let mut buf_c2 = [0u8; 2];
        slice_c.read_exact(&mut buf_c2).unwrap();
        assert_eq!(&buf_c2, b"CC");

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_zero_length_slice() {
        let (path, _temp_dir, slicer) = create_test_file_and_slicer("zero_length.bin");
        let mut z_slice = slicer.get_slice(99).unwrap();

        let mut buf = [0u8; 1];
        let bytes_read = z_slice.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);

        // Seeking to 0 is fine
        assert_eq!(z_slice.seek(SeekFrom::Start(0)).unwrap(), 0);
        assert_eq!(z_slice.seek(SeekFrom::End(0)).unwrap(), 0);

        // Seeking anywhere else is fine for the seek call itself...
        assert_eq!(z_slice.seek(SeekFrom::Start(1)).unwrap(), 1);

        // ... but reading from there will just yield EOF
        let bytes_read_after_seek = z_slice.read(&mut buf).unwrap();
        assert_eq!(bytes_read_after_seek, 0);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_read_past_slice_boundary_is_handled() {
        let (path, _temp_dir, slicer) = create_test_file_and_slicer("read_boundary.bin");
        let mut slice_b = slicer.get_slice(2).unwrap(); // 5 bytes long

        let mut buf = [0u8; 10]; // Buffer is larger than slice
        let bytes_read = slice_b.read(&mut buf).unwrap();

        // Should only read the 5 bytes that are actually in the slice
        assert_eq!(bytes_read, 5);
        assert_eq!(&buf[..5], b"BBBBB");

        // Another read should yield EOF
        assert_eq!(slice_b.read(&mut buf).unwrap(), 0);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_slicer_with_in_memory_cursor() {
        let data = b"HEADER..DATA-SECTION..FOOTER";
        let sections = vec![
            (1, 8, 12), // "DATA-SECTION"
        ]
        .into_iter()
        .map(|(id, offset, size)| FileSliceColumn { id, offset, size })
        .collect();
        let cursor = Cursor::new(data);
        let slicer = FileSlicer::new(cursor, sections);

        let mut data_slice = slicer.get_slice(1).unwrap();
        let mut content = String::new();
        data_slice.read_to_string(&mut content).unwrap();

        assert_eq!(content, "DATA-SECTION");
    }
}
