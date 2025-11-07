pub const DOC_MAGIC: &[u8; 6] = b"MIDOC1";
pub const DOC_HEADER_SIZE: usize = 32; // magic (6) + total_data_size (8) + entry_count (4)
pub const DOC_VERSION: u8 = 1;
