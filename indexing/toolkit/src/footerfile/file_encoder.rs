use crate::footerfile::common::{
    ColumnMeta, Footer, MAGIC_AND_DATA_SIZE, MAGIC_FOOTER, get_footer,
};
use std::fs;
use std::io::Seek;
use std::io::{self, Read, Write};
use std::path::PathBuf;

pub struct FooterFileEncoder {
    footer: Footer,
    file: std::fs::File,
    current_offset: u64,
}

impl FooterFileEncoder {
    pub fn create(path: PathBuf) -> io::Result<Self> {
        Ok(Self {
            footer: Footer {
                magic: *MAGIC_FOOTER,
                size: 0,
                columns: Vec::new(),
            },
            file: fs::File::create(path)?,
            current_offset: 0,
        })
    }

    pub fn open(path: PathBuf) -> io::Result<Self> {
        let mut file = fs::File::open(path)?;
        let file_size = file.metadata()?.len();
        let offset = file_size - (MAGIC_AND_DATA_SIZE as u64);
        file.seek(io::SeekFrom::Start(offset))?;
        let footer = Footer::read_from(&mut file)?;
        file.seek(io::SeekFrom::Start(offset))?;
        Ok(Self {
            footer,
            file,
            current_offset: offset,
        })
    }

    pub fn write<R: io::Read>(&mut self, column_id: u32, reader: &mut R) -> io::Result<()> {
        if self.footer.columns.iter().any(|c| c.id == column_id) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "column already exists",
            ));
        }

        match io::copy(reader, &mut self.file) {
            Ok(size) => {
                self.footer.columns.push(ColumnMeta {
                    id: column_id,
                    offset: self.current_offset,
                    size,
                });
                self.current_offset += size;
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.footer.write_to(&mut self.file)?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::footerfile::file_decoder::FooterFileDecoder;

    use super::*;

    #[test]
    fn test_write() {
        let mut footer = FooterFileEncoder::create(PathBuf::from("test")).unwrap();
        footer
            .write(1, &mut std::io::Cursor::new(b"Hello Rust".to_vec()))
            .expect("err writing hello rust");
        footer
            .write(2, &mut std::io::Cursor::new(b"Hello World".to_vec()))
            .expect("err writing hello rust");
        footer
            .write(3, &mut std::io::Cursor::new(b"Hello World".to_vec()))
            .expect("err writing hello rust");
        footer.close().expect("err writing footer");

        let mut decoder =
            FooterFileDecoder::new(PathBuf::from("test")).expect("err decoding footer");
        let mut column = decoder.get_column(1).expect("err getting column");
        let mut buffer = Vec::new();
        column.read_to_end(&mut buffer).unwrap();
        assert_eq!(buffer, b"Hello Rust");
    }
}
