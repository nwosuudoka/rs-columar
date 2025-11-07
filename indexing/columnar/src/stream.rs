use crate::buffers::smart_pool::SmartBufferPool;
use crate::encoding::StreamingEncoder;
use core::fmt;
use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::path::PathBuf;

pub struct StreamColumn<T> {
    path: PathBuf,
    writer: BufWriter<File>,
    encoder: Box<dyn StreamingEncoder<T>>,
    pool: SmartBufferPool,
    index: Option<Box<dyn FieldIndex<T>>>,
    row_pos: usize,
}

impl<T> fmt::Debug for StreamColumn<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamColumn")
            .field("path", &self.path)
            .finish()
    }
}

pub trait FieldIndex<T> {
    fn record(&mut self, value: &T, position: usize) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
}

impl<T> StreamColumn<T>
where
    T: 'static,
{
    pub fn new<P: Into<PathBuf>>(
        path: P,
        pool: SmartBufferPool,
        encoder: Box<dyn StreamingEncoder<T>>,
        index: Option<Box<dyn FieldIndex<T>>>,
    ) -> io::Result<Self> {
        let path = path.into();

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);
        encoder.begin_stream(&mut writer)?;
        Ok(Self {
            path,
            writer,
            encoder,
            pool,
            row_pos: 0,
            index,
        })
    }

    pub fn push(&mut self, v: &T) -> io::Result<()> {
        self.encoder
            .encode_value(v, self.row_pos, &mut self.writer)?;
        if let Some(index) = &mut self.index {
            index.record(v, self.row_pos)?;
        }
        self.row_pos += 1;
        Ok(())
    }

    pub fn close(mut self) -> io::Result<()> {
        if let Some(mut index) = self.index {
            index.flush()?;
        }
        self.encoder.end_stream(&mut self.writer)
    }
}

pub trait StreamingColumnBundle<Row>: Default {
    fn push(&mut self, row: &Row) -> io::Result<()>;
    fn merge(&mut self, other: Self);
}

pub trait StreamingColumnar: Sized {
    type Columns: StreamingColumnBundle<Self> + Default;

    fn to_streaming_columns(rows: &[Self]) -> Self::Columns {
        let mut cols = Self::Columns::default();
        for r in rows {
            cols.push(r);
        }
        cols
    }
}
