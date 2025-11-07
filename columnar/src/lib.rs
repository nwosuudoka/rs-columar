pub mod buffers;
pub mod encoding;
pub mod generated;
pub mod models;

use crate::buffers::smart_pool::SmartBufferPool;
use crate::encoding::streaming::StreamingEncoder;
pub use columnar_derive::{Columnar, ColumnarAttrs, SimpleColumnar};
use core::fmt;
use std::collections::HashSet;
use std::fs::{self, File};
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
    fn push(&mut self, row: &Row) -> io::Result<()>;
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

#[derive(Debug, Clone)]
pub struct PushConfig {
    allowed_fields: HashSet<String>,
}

impl PushConfig {
    /// Creates a new `PushConfig` with the given set of allowed fields.
    ///
    /// `fields` is an iterator over values that can be converted to `&str`.
    /// The resulting `PushConfig` will allow pushing values to fields that are in the set of
    /// allowed fields, and will forbid pushing to any other fields.
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let allowed_fields = fields.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self { allowed_fields }
    }

    pub fn is_allowed(&self, field: &str) -> bool {
        self.allowed_fields.contains(field)
    }
}

pub trait FilteredPush<Row>: Default {
    fn push_with_config(&mut self, row: &Row, cfg: &crate::PushConfig);
}

pub struct StreamColumn<T> {
    path: PathBuf,
    writer: BufWriter<File>,
    encoder: Box<dyn StreamingEncoder<T>>,
    pool: SmartBufferPool,
    _marker: std::marker::PhantomData<T>,
}

impl<T> fmt::Debug for StreamColumn<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamColumn")
            .field("path", &self.path)
            .finish()
    }
}

impl<T> StreamColumn<T>
where
    T: 'static,
{
    pub fn new<P: Into<PathBuf>>(
        path: P,
        encoder: Box<dyn StreamingEncoder<T>>,
        pool: SmartBufferPool,
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
            _marker: std::marker::PhantomData,
        })
    }

    pub fn push(&mut self, v: &T) -> io::Result<()> {
        self.encoder.encode_value(v, &mut self.writer)
    }

    pub fn merge(&mut self, other: Self) -> io::Result<()> {
        panic!("Not implemented")
    }

    pub fn close(mut self) -> io::Result<()> {
        self.encoder.end_stream(&mut self.writer)
    }
}

pub trait IntoColumns {
    fn to_simple_columns(&self) -> <Self as SimpleColumnar>::Columns
    where
        Self: SimpleColumnar,
    {
        let mut cols = Self::Columns::default();
        cols.push(self);
        cols
    }

    fn to_streaming_columns(&self) -> <Self as StreamingColumnar>::Columns
    where
        Self: StreamingColumnar,
    {
        let mut cols = Self::Columns::default();
        cols.push(self);
        cols
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
        let pool = SmartBufferPool::new(4 * 1024);
        let mut col =
            StreamColumn::new(tmp.path(), Box::new(FixedWidthStreamEncoder), pool).unwrap();
        for i in 0..10u64 {
            col.push(&i).unwrap();
        }
        col.close().unwrap();
    }
}
