use crate::footerfile::common::{Footer, get_footer};
use crate::fspkg::file_slice::FileSlice;
use crate::fspkg::sectioned_slice::{FileSliceColumn, FileSlicer};
use std::io;
use std::{fs, path::PathBuf};

pub struct FooterFileDecoder {
    footer: Footer,
    path: PathBuf,
}

impl FooterFileDecoder {
    /// Opens a file at the given path and reads the footer from it.
    /// Returns an error if the file cannot be opened, or if the footer cannot be read.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, or if the footer cannot be read.
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let mut file = fs::File::open(&path)?;
        let file_size = file.metadata()?.len();
        let (_, footer) = get_footer(&mut file, file_size)?;
        Ok(Self { footer, path })
    }

    /// Returns a `FileSlicer` that provides a view into the file at `path`
    /// that is divided into sections according to the `Footer` stored in `self`.
    ///
    /// The `FileSlicer` contains one section for each column in the `Footer`.
    /// The section at offset `0` contains the data for column with id `0`, and so on.
    ///
    /// # Errors
    ///
    /// Returns an error if the file at `path` cannot be opened.
    ///
    pub fn get(&mut self) -> io::Result<FileSlicer<fs::File>> {
        let file = fs::File::open(&self.path)?;
        let columns = self
            .footer
            .columns
            .iter()
            .map(|c| FileSliceColumn {
                id: c.id,
                offset: c.offset,
                size: c.size,
            })
            .collect();
        Ok(FileSlicer::new(file, columns))
    }

    /// Returns a `FileSlice` that provides a view into the column with id `column_id` in the file at `path`.
    ///
    /// The `FileSlice` contains the data for the column with id `column_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file at `path` cannot be opened, or if the column with id `column_id` is not found.
    pub fn get_column(&mut self, column_id: u32) -> io::Result<FileSlice<fs::File>>
where {
        let column = self.footer.columns.iter().find(|c| c.id == column_id);
        match column {
            Some(column) => {
                let file = fs::File::open(&self.path)?;
                FileSlice::new(file, column.offset, column.size)
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "column not found",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::footerfile::file_encoder::FooterFileEncoder;
    use crate::temp::dir::tempdir;
    use std::io::{Cursor, Read};

    use super::*;

    #[test]
    fn test_column_encode_decode() {
        let temp_dir = tempdir().expect("err creating temp dir");
        let mut encoder = FooterFileEncoder::create(temp_dir.path().join("footer_file"))
            .expect("err crating footer file");

        let actual: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut cursor = Cursor::new(&actual);
        cursor.set_position(0);
        encoder.write(1, &mut cursor).expect("err writing buffer");
        encoder.close().expect("err closing footer file");

        let mut decoder = FooterFileDecoder::new(temp_dir.path().join("footer_file"))
            .expect("err decoding footer");
        let mut column = decoder.get_column(1).expect("err getting column");
        let mut buffer = Vec::new();

        column.read_to_end(&mut buffer).unwrap();
        assert_eq!(buffer, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}
