use super::reader_source::ReaderSource;
use std::fs;
use std::io;

pub trait ReaderSourceProvider {
    fn create_source(&self) -> io::Result<ReaderSource>;
}

pub struct FileCreator {
    filename: String,
}

impl FileCreator {
    pub fn new(filename: String) -> Self {
        return FileCreator { filename };
    }
}

impl ReaderSourceProvider for FileCreator {
    fn create_source(&self) -> io::Result<ReaderSource> {
        let file = fs::File::open(&self.filename)?;
        Ok(ReaderSource::File(file))
    }
}

pub struct MemoryCreator {
    data: Vec<u8>,
}

impl MemoryCreator {
    pub fn new(data: Vec<u8>) -> Self {
        return MemoryCreator { data };
    }
}

impl ReaderSourceProvider for MemoryCreator {
    fn create_source(&self) -> io::Result<ReaderSource> {
        let cursor = io::Cursor::new(self.data.clone());
        Ok(ReaderSource::Cursor(cursor))
    }
}

pub enum SourceProvider {
    File(FileCreator),
    Memory(MemoryCreator),
}

impl SourceProvider {
    pub fn create_reader(&self) -> io::Result<ReaderSource> {
        match self {
            SourceProvider::File(file_creator) => file_creator.create_source(),
            SourceProvider::Memory(memory_creator) => memory_creator.create_source(),
        }
    }
}
