pub mod encoding;
pub mod example;
pub mod generated;
pub mod models;

pub use columnar_derive::{Columnar, SimpleColumnar};
use core::fmt;
use std::any::TypeId;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
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

/// Trait for streaming encoders: stateful, incremental encoders that
/// can write data as it arrives.
pub trait StreamingEncoder<T>: Send + Sync {
    fn begin_stream(&self, writer: &mut dyn Write) -> io::Result<()>;
    fn encode_value(&self, v: &T, writer: &mut dyn Write) -> io::Result<()>;
    fn end_stream(&self, writer: &mut dyn Write) -> io::Result<()>;
}

/// Common built-in encoders
pub mod encoders {
    use super::StreamingEncoder;
    use std::io::Write;

    /// Writes each value as fixed-width binary (e.g., 8 bytes for u64).
    pub struct FixedWidthStreamEncoder;

    impl<T: Copy> StreamingEncoder<T> for FixedWidthStreamEncoder {
        fn begin_stream(&self, _writer: &mut dyn Write) -> std::io::Result<()> {
            Ok(())
        }
        fn encode_value(&self, v: &T, writer: &mut dyn Write) -> std::io::Result<()> {
            let bytes = unsafe {
                std::slice::from_raw_parts((v as *const T) as *const u8, std::mem::size_of::<T>())
            };
            writer.write_all(bytes)
        }
        fn end_stream(&self, _writer: &mut dyn Write) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Delta encoding for monotonic integers.
    pub struct DeltaStreamEncoder {
        prev: std::sync::Mutex<Option<i64>>,
    }

    impl Default for DeltaStreamEncoder {
        fn default() -> Self {
            Self {
                prev: std::sync::Mutex::new(None),
            }
        }
    }

    impl DeltaStreamEncoder {
        pub fn new() -> Self {
            Self::default()
        }
    }

    impl StreamingEncoder<i64> for DeltaStreamEncoder {
        fn begin_stream(&self, _writer: &mut dyn Write) -> std::io::Result<()> {
            Ok(())
        }

        fn encode_value(&self, v: &i64, writer: &mut dyn Write) -> std::io::Result<()> {
            let mut guard = self.prev.lock().unwrap();
            let delta = match *guard {
                None => *v,
                Some(prev) => *v - prev,
            };
            writer.write_all(&delta.to_le_bytes())?;
            *guard = Some(*v);
            Ok(())
        }

        fn end_stream(&self, _writer: &mut dyn Write) -> std::io::Result<()> {
            Ok(())
        }
    }
}

//
// ===========================================================
//  5️⃣ ENCODER FACTORY (OPTIONAL PLUGGABLE REGISTRY)
// ===========================================================
//

#[derive(Default)]
pub struct EncoderFactory {
    encoders: HashMap<TypeId, Box<dyn Fn() -> Box<dyn std::any::Any + Send + Sync>>>,
}

impl EncoderFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T: 'static>(
        &mut self,
        f: impl Fn() -> Box<dyn crate::StreamingEncoder<T>> + 'static,
    ) {
        self.encoders.insert(
            TypeId::of::<T>(),
            Box::new(move || {
                let enc = f();
                Box::new(enc) as Box<dyn std::any::Any + Send + Sync>
            }),
        );
    }

    pub fn get<T: 'static>(&self) -> Option<Box<dyn crate::StreamingEncoder<T>>> {
        self.encoders.get(&TypeId::of::<T>()).and_then(|f| {
            f().downcast::<Box<dyn crate::StreamingEncoder<T>>>()
                .ok()
                .map(|boxed| *boxed)
        })
    }
}

/// Returns a global factory with sensible defaults.
pub fn default_factory() -> EncoderFactory {
    let mut f = EncoderFactory::new();
    f.register::<i64>(|| Box::new(encoders::DeltaStreamEncoder::new()));
    f.register::<u64>(|| Box::new(encoders::FixedWidthStreamEncoder));
    f
}

//
// ===========================================================
//  6️⃣ TESTS (Quick Validation)
// ===========================================================
//

#[cfg(test)]
mod tests {
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
        let mut col =
            StreamColumn::new(tmp.path(), Box::new(encoders::FixedWidthStreamEncoder)).unwrap();
        for i in 0..10u64 {
            col.push(&i).unwrap();
        }
        col.close().unwrap();
    }
}
