pub mod example;
pub use columnar_derive::{Columnar, SimpleColumnar};
use std::default::Default;

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
        } // sensible default
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
            .map_or(true, |c| c.len() == self.chunk_size)
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

// Bundle of columns for a Row type
pub trait ColumnStorageBundle<Row>: Default {
    fn push(&mut self, row: &Row);
    fn merge(&mut self, other: Self);
    fn set_chunk_size(&mut self, n: usize);
}

// Row ↔ Columns relationship
pub trait Columnar: Sized {
    type Columns: ColumnStorageBundle<Self>;

    fn to_columns(rows: &[Self]) -> Self::Columns {
        let mut cols = Self::Columns::default();
        for r in rows {
            cols.push(r);
        }
        cols
    }
}
