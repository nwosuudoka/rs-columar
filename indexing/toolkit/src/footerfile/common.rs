use std::io;

pub const MAGIC_FOOTER: &[u8; 6] = b"FOOTR1";
pub const MAGIC_AND_DATA_SIZE: usize = 14;
const COLUMN_META_SIZE: usize = 20;

#[derive(Debug, PartialEq)]
pub struct ColumnMeta {
    pub id: u32,
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, PartialEq)]
pub struct Footer {
    pub columns: Vec<ColumnMeta>,
    pub size: u64,
    pub magic: [u8; 6],
}

impl Footer {
    pub fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buffer = Vec::new();
        let mut size = 0;
        for column in &self.columns {
            buffer.extend_from_slice(column.id.to_le_bytes().as_slice());
            buffer.extend_from_slice(column.offset.to_le_bytes().as_slice());
            buffer.extend_from_slice(column.size.to_le_bytes().as_slice());
            size += COLUMN_META_SIZE;
        }

        buffer.extend_from_slice(size.to_le_bytes().as_slice());
        buffer.extend_from_slice(&self.magic);
        writer.write_all(&buffer)?;
        Ok(())
    }

    pub fn read_from<T: io::Read>(reader: &mut T) -> io::Result<Footer> {
        let mut vec = Vec::new();
        reader.read_to_end(&mut vec)?;
        Self::read_from_buffer(&vec)
    }

    fn read_from_buffer(buff: &[u8]) -> io::Result<Footer> {
        if &buff[buff.len() - 6..] != MAGIC_FOOTER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "invalid magic number {:?} != {:?}",
                    &buff[buff.len() - 6..],
                    MAGIC_FOOTER
                ),
            ));
        }

        let start = buff.len() - MAGIC_AND_DATA_SIZE;
        let end = buff.len() - MAGIC_FOOTER.len();
        let size = u64::from_le_bytes(buff[start..end].try_into().unwrap());

        let columns = buff[..start]
            .chunks(20)
            .map(|chunk| {
                let id = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
                let offset = u64::from_le_bytes(chunk[4..12].try_into().unwrap());
                let size = u64::from_le_bytes(chunk[12..20].try_into().unwrap());
                ColumnMeta { id, offset, size }
            })
            .collect();
        Ok(Footer {
            columns,
            size,
            magic: *MAGIC_FOOTER,
        })
    }
}

pub fn get_footer<T>(read_seeker: &mut T, file_size: u64) -> io::Result<(u64, Footer)>
where
    T: io::Read + io::Seek,
{
    let mut buff = Vec::with_capacity(MAGIC_AND_DATA_SIZE);
    read_seeker.seek(io::SeekFrom::Start(
        file_size - (MAGIC_AND_DATA_SIZE as u64),
    ))?;
    read_seeker.read_to_end(&mut buff)?;

    let size = u64::from_le_bytes(buff[0..8].try_into().unwrap());
    if &buff[8..MAGIC_AND_DATA_SIZE] != MAGIC_FOOTER {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "invalid magic number {:?} != {:?}",
                &buff[8..MAGIC_AND_DATA_SIZE],
                MAGIC_FOOTER
            ),
        ));
    }

    let offset = file_size - (size + (MAGIC_AND_DATA_SIZE as u64));
    read_seeker.seek(io::SeekFrom::Start(offset))?;
    buff.resize(size as usize, 0);
    read_seeker.read_exact(&mut buff)?;

    let columns = buff
        .chunks(COLUMN_META_SIZE)
        .map(|chunk| {
            let id = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
            let offset = u64::from_le_bytes(chunk[4..12].try_into().unwrap());
            let size = u64::from_le_bytes(chunk[12..20].try_into().unwrap());
            ColumnMeta { id, offset, size }
        })
        .collect::<Vec<ColumnMeta>>();

    Ok((
        offset,
        Footer {
            magic: *MAGIC_FOOTER,
            size,
            columns,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_buffer_read_write() {
        let footer = Footer {
            columns: vec![ColumnMeta {
                id: 1,
                offset: 0,
                size: 0,
            }],
            size: COLUMN_META_SIZE as u64,
            magic: *MAGIC_FOOTER,
        };
        let mut vec = Vec::new();
        footer.write_to(&mut vec).expect("err writing to vec");
        let footer2 = Footer::read_from_buffer(&vec).expect("err reading from vec");
        assert_eq!(footer, footer2);
    }
}
