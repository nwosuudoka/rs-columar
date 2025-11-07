use std::io;

pub trait LeNum: Sized + Copy + Ord {
    fn from_le_bytes(slice: &[u8]) -> Self;
    fn to_le_bytes(self) -> Vec<u8>;
}

macro_rules! impl_num_for_primitive {
    ($($t:ty),*) => {
        $(
        impl LeNum for $t {
            #[inline(always)]
            fn from_le_bytes(slice: &[u8]) -> Self {
                Self::from_le_bytes(slice.try_into().expect("slice with incorrect length"))
            }

            #[inline(always)]
            fn to_le_bytes(self) -> Vec<u8> {
                self.to_le_bytes().to_vec()
            }
        })*
    };
}

impl_num_for_primitive!(u8, u16, u32, u64, i8, i16, i32, i64, usize, isize);

pub struct NumReadIter<R, T>
where
    T: LeNum,
    R: io::Read,
{
    reader: R,
    _marker: std::marker::PhantomData<T>,
}

impl<R, T> NumReadIter<R, T>
where
    T: LeNum,
    R: io::Read,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<R, T> Iterator for NumReadIter<R, T>
where
    T: LeNum,
    R: io::Read,
{
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = vec![0u8; std::mem::size_of::<T>()];
        match self.reader.read_exact(&mut buf) {
            Ok(_) => Some(Ok(T::from_le_bytes(&buf))),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct NumWriteIter<W, T>
where
    T: LeNum,
    W: io::Write,
{
    writer: W,
    _marker: std::marker::PhantomData<T>,
}

impl<W, T> NumWriteIter<W, T>
where
    T: LeNum,
    W: io::Write,
{
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn write(&mut self, value: T) -> io::Result<()> {
        let bytes = value.to_le_bytes();
        self.writer.write_all(&bytes)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
