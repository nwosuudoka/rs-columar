pub const MAGIC: u64 = 0xABCFDFF;
pub const HEADER_SIZE: usize = 32;
pub const ROW_OFFSET_SIZE: usize = 12;

use std::mem;

#[derive(Debug, PartialEq)]
pub struct OffsetHeader<T: IsAllowedId> {
    pub offset: u64,
    pub id: T,
    pub size: u32,
}

impl<T: IsAllowedId> OffsetHeader<T> {
    pub fn from_buffer(buffer: &[u8]) -> Result<Self, &'static str> {
        let mut off: usize = 0;
        let id_size = T::byte_size();
        let offset = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
        off += 8;
        let id = T::from_le_bytes(&buffer[off..off + id_size])?;
        off += id_size;
        let size = u32::from_le_bytes(buffer[off..off + 4].try_into().unwrap());
        Ok(OffsetHeader { offset, id, size })
    }

    pub fn write_to_buffer(&self, buffer: &mut [u8]) {
        buffer[0..8].copy_from_slice(&self.offset.to_le_bytes());
        let id_start = 8;
        let id_size = T::byte_size();
        self.id
            .write_le_bytes(&mut buffer[id_start..id_start + id_size]);
        let size_start = id_start + id_size;
        buffer[size_start..size_start + 4].copy_from_slice(&self.size.to_le_bytes());
    }

    pub fn size() -> usize {
        8 + T::byte_size() + 4
    }
}

mod private {
    pub trait Sealed {}
}

pub trait IsAllowedId: private::Sealed + Copy + std::fmt::Debug + std::cmp::PartialEq {
    fn byte_size() -> usize;
    fn to_u64(self) -> u64;
    fn write_le_bytes(self, slice: &mut [u8]);
    fn from_le_bytes(bytes: &[u8]) -> Result<Self, &'static str>;
    fn get_le_bytes(&self) -> Vec<u8>;
}

// impl

impl private::Sealed for u16 {}

impl IsAllowedId for u16 {
    fn byte_size() -> usize {
        mem::size_of::<u16>()
    }
    fn to_u64(self) -> u64 {
        self as u64
    }
    fn write_le_bytes(self, slice: &mut [u8]) {
        slice.copy_from_slice(&self.to_le_bytes());
    }

    fn from_le_bytes(slice: &[u8]) -> Result<Self, &'static str> {
        let array = slice
            .try_into()
            .map_err(|_| "Slice does not have length 2")?;
        Ok(u16::from_le_bytes(array))
    }

    fn get_le_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl private::Sealed for u32 {}
impl IsAllowedId for u32 {
    fn byte_size() -> usize {
        mem::size_of::<u32>()
    }
    fn to_u64(self) -> u64 {
        self as u64
    }
    fn write_le_bytes(self, slice: &mut [u8]) {
        slice.copy_from_slice(&self.to_le_bytes());
    }
    fn from_le_bytes(slice: &[u8]) -> Result<Self, &'static str> {
        let array = slice
            .try_into()
            .map_err(|_| "Slice does not have length 4")?;
        Ok(u32::from_le_bytes(array))
    }

    fn get_le_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl private::Sealed for u64 {}
impl IsAllowedId for u64 {
    fn byte_size() -> usize {
        mem::size_of::<u64>()
    }
    fn to_u64(self) -> u64 {
        self
    }
    fn write_le_bytes(self, slice: &mut [u8]) {
        slice.copy_from_slice(&self.to_le_bytes());
    }

    fn from_le_bytes(slice: &[u8]) -> Result<Self, &'static str> {
        let array = slice
            .try_into()
            .map_err(|_| "Slice does not have length 8")?;
        Ok(u64::from_le_bytes(array))
    }

    fn get_le_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
