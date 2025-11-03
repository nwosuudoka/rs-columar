pub mod encoding;
pub mod generated;
pub mod models;

use crate::encoding::streaming::StreamingEncoder;
pub use columnar_derive::{Columnar, SimpleColumnar};
use core::fmt;
use std::fs::File;
use std::io::{self, BufWriter};
use std::{default::Default, path::PathBuf};

pub trait Columnar: Sized {
    type Columns: ColumnBundle<Self> + Default;

    fn to_columns(rows: &[Self]) -> Self::Columns {
        let mut cols = Self::Columns::default();
        for r in rows {
            cols.push(r);
        }
        cols
    }
}

pub trait ColumnBundle<Row>: Default {
    fn push(&mut self, row: &Row);
    fn merge(&mut self, other: Self);
    fn set_chunk_size(&mut self, n: usize) {
        let _ = n;
    }
}

pub trait SimpleColumnBundle<Row>: Default {
    fn push(&mut self, row: &Row);
    fn merge(&mut self, other: Self);
}

pub trait StreamingColumnBundle<Row>: Default {
    fn push(&mut self, row: &Row);
    fn merge(&mut self, other: Self);
}

pub trait SimpleColumnar: Sized {
    type Columns: SimpleColumnBundle<Self> + Default;

    fn to_simple_columns(rows: &[Self]) -> Self::Columns {
        let mut cols = Self::Columns::default();
        for r in rows {
            cols.push(r);
        }
        cols
    }
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

/// Simple Vec-backed column, mostly for testing or light use.
#[derive(Debug, Default, Clone)]
pub struct VecColumn<T>(pub Vec<T>);

impl<T: Clone> VecColumn<T> {
    pub fn push(&mut self, v: &T) {
        self.0.push(v.clone());
    }
    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}
// A single typed, chunked column
#[derive(Debug)]
pub struct Column<T> {
    pub chunks: Vec<Vec<T>>,
    pub chunk_size: usize,
}

impl<T> Default for Column<T> {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            chunk_size: 1_000_000,
        }
    }
}

impl<T: Clone> Column<T> {
    pub fn with_chunk_size(mut self, n: usize) -> Self {
        self.chunk_size = n;
        self
    }

    pub fn push(&mut self, v: &T) {
        if self
            .chunks
            .last()
            .is_none_or(|c| c.len() == self.chunk_size)
        {
            self.chunks.push(Vec::with_capacity(self.chunk_size));
        }
        self.chunks.last_mut().unwrap().push(v.clone());
    }

    pub fn len(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn extend_from(&mut self, other: &Self)
    where
        T: Clone,
    {
        for chunk in &other.chunks {
            self.chunks.push(chunk.clone());
        }
    }
}

pub struct StreamColumn<T> {
    path: PathBuf,
    writer: BufWriter<File>,
    encoder: Box<dyn StreamingEncoder<T>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> fmt::Debug for StreamColumn<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamColumn")
            .field("path", &self.path)
            .finish()
    }
}

impl<T> StreamColumn<T> {
    pub fn new<P: Into<PathBuf>>(
        path: P,
        encoder: Box<dyn StreamingEncoder<T>>,
    ) -> io::Result<Self> {
        let path = path.into();
        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);
        encoder.begin_stream(&mut writer)?;
        Ok(Self {
            path,
            writer,
            encoder,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn push(&mut self, v: &T) -> io::Result<()> {
        self.encoder.encode_value(v, &mut self.writer)
    }

    pub fn close(mut self) -> io::Result<()> {
        self.encoder.end_stream(&mut self.writer)
    }
}

//
// ===========================================================
//  6️⃣ TESTS (Quick Validation)
// ===========================================================
//

#[cfg(test)]
mod tests {
    use crate::encoding::FixedWidthStreamEncoder;

    use super::*;

    #[test]
    fn test_vec_column_merge() {
        let mut a = Column::default().with_chunk_size(2);
        a.push(&1);
        a.push(&2);
        let mut b = Column::default().with_chunk_size(2);
        b.push(&3);
        b.push(&4);

        a.extend_from(&b);
        assert_eq!(a.len(), 4);
    }

    #[test]
    fn test_stream_column_write() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut col = StreamColumn::new(tmp.path(), Box::new(FixedWidthStreamEncoder)).unwrap();
        for i in 0..10u64 {
            col.push(&i).unwrap();
        }
        col.close().unwrap();
    }
}
