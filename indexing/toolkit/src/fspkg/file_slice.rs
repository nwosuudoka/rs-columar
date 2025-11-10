use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

/// A struct that provides a file-like view into a subsection of another readable and seekable source.
///
/// It wraps an inner reader (like a `File`) and confines all `Read` and `Seek`
/// operations to a specified range. The slice is defined by an absolute `start_offset`
/// within the inner reader and a `size`, which is the length of the slice.
///
/// All `Read` and `Seek` operations on `FileSlice` are relative to the start of the slice,
/// not the start of the underlying file.
pub struct FileSlice<F>
where
    F: Read + Seek,
{
    inner: F,
    /// The absolute starting position of the slice in the inner reader.
    start_offset: u64,
    /// The total size (length) of this slice in bytes.
    size: u64,
    /// The current read/seek position, relative to the start of the slice.
    current_pos: u64,
}

impl<F> FileSlice<F>
where
    F: Read + Seek,
{
    /// Creates a new `FileSlice`.
    ///
    /// This will create a view into the `inner` reader starting at `offset` and spanning `size` bytes.
    ///
    /// # Errors
    /// Returns an error if the initial seek to the start of the slice fails.
    pub fn new(mut inner: F, offset: u64, size: u64) -> io::Result<Self> {
        // Position the underlying file reader at the absolute start of our slice.
        inner.seek(SeekFrom::Start(offset))?;

        Ok(Self {
            inner,
            start_offset: offset,
            size,           // Store the size directly.
            current_pos: 0, // Our own relative position starts at 0.
        })
    }

    /// Returns the total size of this slice.
    pub fn len(&self) -> u64 {
        self.size
    }
}

impl<F> Read for FileSlice<F>
where
    F: Read + Seek,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Calculate how many bytes are left to read within our slice's bounds.
        let bytes_left = self.size - self.current_pos;
        if bytes_left == 0 {
            return Ok(0); // End of slice.
        }

        // Limit the read to the smaller of the buffer's capacity or the remaining bytes in the slice.
        let bytes_to_read = std::cmp::min(buf.len(), bytes_left as usize);
        let limited_buf = &mut buf[..bytes_to_read];

        // Read from the inner source.
        let num_bytes_read = self.inner.read(limited_buf)?;

        // Advance our relative position.
        self.current_pos += num_bytes_read as u64;

        Ok(num_bytes_read)
    }
}

impl<F> Seek for FileSlice<F>
where
    F: Read + Seek,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // Calculate the new target position *relative to the start of the slice*.
        // We use i64 to correctly handle negative offsets from SeekFrom::End or SeekFrom::Current.
        let new_pos_relative: i64 = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(p) => self.size as i64 + p,
            SeekFrom::Current(p) => self.current_pos as i64 + p,
        };

        // The new relative position must be within the slice's bounds [0, self.size].
        if new_pos_relative < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start of slice",
            ));
        }

        let new_pos_relative = new_pos_relative as u64;

        if new_pos_relative > self.size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek beyond end of slice",
            ));
        }

        // Calculate the corresponding absolute position in the underlying file.
        let new_pos_absolute = self.start_offset + new_pos_relative;

        // Perform the actual seek on the inner file.
        self.inner.seek(SeekFrom::Start(new_pos_absolute))?;

        // If the seek was successful, update our internal relative position.
        self.current_pos = new_pos_relative;

        // Return the new relative position, as required by the Seek trait.
        Ok(self.current_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp::dir::TempDir;
    use std::fs::File;
    use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;

    /// A test fixture that creates a file with known content.
    /// The content is "0123456789abcdefghijklmnopqrstuvwxyz" (36 bytes).
    fn create_test_file(name: &str) -> io::Result<(PathBuf, TempDir)> {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join(name);
        let mut file = File::create(&path)?;
        file.write_all(b"0123456789abcdefghijklmnopqrstuvwxyz")?;
        file.flush()?;
        Ok((path, temp_dir))
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Basic Read Tests
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    #[test]
    fn test_read_full_slice() {
        let path = create_test_file("read_full.bin").unwrap();
        let file = File::open(&path.0).unwrap();

        // Slice 'a' through 'j' (offset 10, size 10)
        let mut slice = FileSlice::new(file, 10, 10).unwrap();
        let mut content = String::new();
        let bytes_read = slice.read_to_string(&mut content).unwrap();

        assert_eq!(bytes_read, 10);
        assert_eq!(content, "abcdefghij");
    }

    #[test]
    fn test_read_in_chunks() {
        let path = create_test_file("read_chunks.bin").unwrap();
        let file = File::open(&path.0).unwrap();
        let mut slice = FileSlice::new(file, 20, 10).unwrap(); // 'k' through 't'

        let mut buf = [0u8; 5];

        // Read first 5 bytes
        let bytes_read1 = slice.read(&mut buf).unwrap();
        assert_eq!(bytes_read1, 5);
        assert_eq!(&buf, b"klmno");

        // Read next 5 bytes
        let bytes_read2 = slice.read(&mut buf).unwrap();
        assert_eq!(bytes_read2, 5);
        assert_eq!(&buf, b"pqrst");

        // Try to read again, should be EOF for the slice
        let bytes_read3 = slice.read(&mut buf).unwrap();
        assert_eq!(bytes_read3, 0);

        // std::fs::remove_file(path.0).unwrap();
    }

    #[test]
    fn test_read_exact() {
        let path = create_test_file("read_exact.bin").unwrap();
        let file = File::open(&path.0).unwrap();
        let mut slice = FileSlice::new(file, 0, 10).unwrap(); // '0' through '9'

        let mut buf = [0u8; 10];
        slice.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"0123456789");
    }

    #[test]
    fn test_read_exact_past_end_of_slice_fails() {
        let path = create_test_file("read_exact_fail.bin").unwrap();
        let file = File::open(&path.0).unwrap();
        let mut slice = FileSlice::new(file, 30, 6).unwrap(); // 'u' through 'z'

        let mut buf = [0u8; 7]; // Try to read 7 bytes from a 6-byte slice
        let result = slice.read_exact(&mut buf);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Seek Tests
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    #[test]
    fn test_seek_and_read() {
        let path = create_test_file("seek_read.bin").unwrap();
        let file = File::open(&path.0).unwrap();
        let mut slice = FileSlice::new(file, 10, 10).unwrap(); // "abcdefghij"

        // Seek from start
        let new_pos = slice.seek(SeekFrom::Start(5)).unwrap();
        assert_eq!(new_pos, 5);

        let mut buf = [0u8; 5];
        slice.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"fghij");

        // Seek from end
        let new_pos = slice.seek(SeekFrom::End(-2)).unwrap();
        assert_eq!(new_pos, 8);

        let mut buf2 = [0u8; 2];
        slice.read_exact(&mut buf2).unwrap();
        assert_eq!(&buf2, b"ij");

        // Seek from current
        slice.seek(SeekFrom::Start(2)).unwrap(); // Go to 'c'
        slice.seek(SeekFrom::Current(3)).unwrap(); // Move 3 forward to 'f'

        let mut buf3 = [0u8; 3];
        slice.read_exact(&mut buf3).unwrap();
        assert_eq!(&buf3, b"fgh");

        // std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_seek_to_boundaries() {
        let path = create_test_file("seek_boundaries.bin").unwrap();
        let file = File::open(&path.0).unwrap();
        let mut slice = FileSlice::new(file, 5, 15).unwrap();

        // Seek to the very end
        let pos = slice.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(pos, 15);
        assert_eq!(slice.current_pos, 15);

        // Reading at the end should yield 0 bytes
        let mut buf = [0u8; 1];
        let bytes_read = slice.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);

        // Seek to the very start
        let pos = slice.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(pos, 0);
        assert_eq!(slice.current_pos, 0);
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Error Handling and Edge Case Tests
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    #[test]
    fn test_seek_before_start_of_slice_fails() {
        let path = create_test_file("seek_before.bin").unwrap();
        let file = File::open(&path.0).unwrap();
        let mut slice = FileSlice::new(file, 10, 10).unwrap();

        let result = slice.seek(SeekFrom::Start(u64::MAX)); // Effectively a negative seek
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);

        let result2 = slice.seek(SeekFrom::End(-11));
        assert!(result2.is_err());
        assert_eq!(result2.unwrap_err().kind(), io::ErrorKind::InvalidInput);

        let result3 = slice.seek(SeekFrom::Current(-1));
        assert!(result3.is_err());
        assert_eq!(result3.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_seek_after_end_of_slice_fails() {
        let path = create_test_file("seek_after.bin").unwrap();
        let file = File::open(&path.0).unwrap();
        let mut slice = FileSlice::new(file, 10, 10).unwrap();

        let result = slice.seek(SeekFrom::Start(11));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);

        let result2 = slice.seek(SeekFrom::End(1));
        assert!(result2.is_err());
        assert_eq!(result2.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn test_zero_length_slice() {
        let path = create_test_file("zero_length.bin").unwrap();
        let file = File::open(&path.0).unwrap();
        let mut slice = FileSlice::new(file, 10, 0).unwrap();

        assert_eq!(slice.len(), 0);

        let mut buf = [0u8; 5];
        let bytes_read = slice.read(&mut buf).unwrap();
        assert_eq!(bytes_read, 0);

        // Can only seek to the start/end
        assert_eq!(slice.seek(SeekFrom::Start(0)).unwrap(), 0);
        assert!(slice.seek(SeekFrom::Start(1)).is_err());
    }

    #[test]
    fn test_slice_of_in_memory_cursor() {
        let data = b"0123456789abcdef";
        let cursor = Cursor::new(data);

        // Create a slice from 'a' to 'f'
        let mut slice = FileSlice::new(cursor, 10, 6).unwrap();
        let mut content = String::new();
        slice.read_to_string(&mut content).unwrap();

        assert_eq!(content, "abcdef");
    }

    #[test]
    fn test_multiple_slices_on_same_file_are_independent() {
        let path = create_test_file("multiple_slices.bin").unwrap();

        // Open two separate file handles to the same file
        let file1 = File::open(&path.0).unwrap();
        let file2 = File::open(&path.0).unwrap();

        // Slice 'A' covers the numbers, Slice 'B' covers letters
        let mut slice_a = FileSlice::new(file1, 0, 10).unwrap(); // "0123456789"
        let mut slice_b = FileSlice::new(file2, 10, 26).unwrap(); // "abc...xyz"

        let mut buf_a = [0u8; 5];
        slice_a.read_exact(&mut buf_a).unwrap();
        assert_eq!(&buf_a, b"01234");

        // Current position of slice_a should be 5
        assert_eq!(slice_a.seek(SeekFrom::Current(0)).unwrap(), 5);

        let mut buf_b = [0u8; 5];
        slice_b.read_exact(&mut buf_b).unwrap();
        assert_eq!(&buf_b, b"abcde");

        // Current position of slice_b should be 5, unaffected by slice_a
        assert_eq!(slice_b.seek(SeekFrom::Current(0)).unwrap(), 5);
    }
}
