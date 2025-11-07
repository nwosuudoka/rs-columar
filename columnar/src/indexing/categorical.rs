use crate::{FieldIndex, encoding::bitpack::v1::common::BitEncodable};
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    fs::File,
    hash::Hash,
    io::{self, BufReader},
};

struct Categorical<T> {
    path: String,
    table: HashMap<T, RoaringBitmap>,
}

impl<T> Categorical<T>
where
    T: Clone,
{
    pub fn new(path: &str) -> Self {
        Self {
            table: HashMap::new(),
            path: path.to_string(),
        }
    }
}

impl<T> FieldIndex<T> for Categorical<T>
where
    T: Clone + Hash + Eq,
{
    fn record(&mut self, value: &T, position: usize) -> std::io::Result<()> {
        self.table
            .entry(value.clone())
            .or_insert_with(RoaringBitmap::new)
            .insert(position as u32);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let file = File::create(&self.path)?;
        let buffered_file = BufReader::new(file);
        for (value, bitmap) in &self.table {
            // let mut buf = Vec::new();
            // buf.extend_from_slice(&value.to_be_bytes());
            // buf.extend_from_slice(&bitmap.to_bytes());
        }
        Ok(())
    }
}
