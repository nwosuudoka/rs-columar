use std::fs;
use std::io::{self, Cursor, Read, Seek};

#[derive(Debug)]
pub enum ReaderSource {
    File(fs::File),
    Cursor(Cursor<Vec<u8>>),
}

impl Read for ReaderSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            ReaderSource::File(file) => file.read(buf),
            ReaderSource::Cursor(cursor) => cursor.read(buf),
        }
    }
}

impl Seek for ReaderSource {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        match self {
            ReaderSource::File(file) => file.seek(pos),
            ReaderSource::Cursor(cursor) => cursor.seek(pos),
        }
    }
}
