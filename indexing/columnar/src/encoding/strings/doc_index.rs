use crate::{FieldIndex, encoding::strings::common::process_string};
use std::{collections::HashMap, fs, io, path::PathBuf};

struct TokenPos {
    token: u64,
    pos: u32,
    freq: u32,
}

pub struct DocIndex {
    temp_dir: PathBuf,
    index_path: PathBuf,
    writers: Option<Vec<io::BufWriter<fs::File>>>,
}

impl DocIndex {
    pub fn new(temp_dir: PathBuf, index_path: PathBuf) -> Self {
        Self {
            temp_dir,
            index_path,
            writers: None,
        }
    }
}

impl FieldIndex<String> for DocIndex {
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn record(&mut self, value: &String, position: usize) -> std::io::Result<()> {
        let tokens = process_string(value);
        match &mut self.writers {
            Some(writers) => {
                // let writer = writers.entry(position as u8).or_default();
                // writer.write_all(value.as_bytes())?;
            }
            None => {
                let writers = (0..8)
                    .map(|i| {
                        let path = self.temp_dir.join(format!("doc_writer_{}.bin", i));
                        let file = fs::File::create(&path).unwrap();
                        io::BufWriter::new(file)
                    })
                    .collect::<Vec<io::BufWriter<fs::File>>>();
                self.writers = Some(writers);
            }
        }

        Ok(())
    }
}
