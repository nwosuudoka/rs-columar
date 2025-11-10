use std::io;

pub trait ReadSeeker: io::Read + io::Seek {}
impl<T: io::Read + io::Seek> ReadSeeker for T {}
