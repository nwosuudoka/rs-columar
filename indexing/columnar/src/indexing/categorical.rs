use crate::{FieldIndex, encoding::strings::common::hash_string};
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    fs::{self, File},
    hash::Hash,
    path::PathBuf,
};
use toolkit::table::encoder;

pub trait CatIntConv {
    fn into_u64(self) -> u64;
}

macro_rules! impl_conv_int {
    ($($t:ty),*) => {
        $(
            impl CatIntConv for $t {
                fn into_u64(self) -> u64 {
                    self as u64
                }
            }
        )*
    };
}

impl_conv_int!(u8, u16, u32, u64, i8, i16, i32, i64, isize, usize);

impl CatIntConv for String {
    fn into_u64(self) -> u64 {
        hash_string(self.as_str())
    }
}

struct Categorical<T> {
    temp_dir: PathBuf,
    path: PathBuf,
    table: HashMap<T, RoaringBitmap>,
}

impl<T> Categorical<T>
where
    T: Clone,
{
    pub fn new(temp_dir: PathBuf, path: PathBuf) -> Self {
        Self {
            table: HashMap::new(),
            path,
            temp_dir,
        }
    }
}

impl<T> FieldIndex<T> for Categorical<T>
where
    T: Clone + Hash + Eq,
    T: CatIntConv,
{
    fn record(&mut self, value: &T, position: usize) -> std::io::Result<()> {
        self.table
            .entry(value.clone())
            .or_default()
            .insert(position as u32);
        Ok(())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let table_dir = self.temp_dir.join("table");
        fs::create_dir_all(&table_dir)?;
        let mut table = encoder::Encoder::<u64>::new(table_dir.clone())?;

        let mut vec = Vec::new();
        for (key, bitmap) in &self.table {
            bitmap.serialize_into(&mut vec)?;
            table.write(key.clone().into_u64(), &vec)?;
        }
        drop(vec);

        let mut file = File::create(&self.path)?;
        table.export(&mut file)
    }
}
