use crate::buffers::smart_pool::SmartBufferPool;
use crate::encoding::{self, streaming::StreamingEncoder};
use std::io;
use std::{any::TypeId, collections::HashMap};

#[derive(Default)]
pub struct EncoderFactory {
    encoders: HashMap<TypeId, Box<dyn Fn() -> Box<dyn std::any::Any + Send + Sync>>>,
}

impl EncoderFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T: 'static>(
        &mut self,
        f: impl Fn() -> Box<dyn crate::StreamingEncoder<T>> + 'static,
    ) {
        self.encoders.insert(
            TypeId::of::<T>(),
            Box::new(move || {
                let enc = f();
                Box::new(enc) as Box<dyn std::any::Any + Send + Sync>
            }),
        );
    }

    pub fn get<T: 'static>(&self) -> Option<Box<dyn StreamingEncoder<T>>> {
        self.encoders.get(&TypeId::of::<T>()).and_then(|f| {
            f().downcast::<Box<dyn StreamingEncoder<T>>>()
                .ok()
                .map(|boxed| *boxed)
        })
    }
}

pub fn default_factory(pool: SmartBufferPool) -> EncoderFactory {
    let mut f = EncoderFactory::new();
    {
        let p = pool.clone();
        f.register::<i8>(move || Box::new(encoding::BitpackStreamWriter::<i8>::new(p.clone())));
    }
    {
        let p = pool.clone();
        f.register::<u8>(move || Box::new(encoding::BitpackStreamWriter::<u8>::new(p.clone())));
    }
    {
        let p = pool.clone();
        f.register::<i16>(move || Box::new(encoding::BitpackStreamWriter::<i16>::new(p.clone())));
    }
    {
        let p = pool.clone();
        f.register::<u16>(move || Box::new(encoding::BitpackStreamWriter::<u16>::new(p.clone())));
    }
    {
        let p = pool.clone();
        f.register::<i32>(move || Box::new(encoding::BitpackStreamWriter::<i32>::new(p.clone())));
    }
    {
        let p = pool.clone();
        f.register::<u32>(move || Box::new(encoding::BitpackStreamWriter::<u32>::new(p.clone())));
    }
    {
        let p = pool.clone();
        f.register::<i64>(move || Box::new(encoding::BitpackStreamWriter::<i64>::new(p.clone())));
    }
    {
        let p = pool.clone();
        f.register::<u64>(move || Box::new(encoding::BitpackStreamWriter::<u64>::new(p.clone())));
    }
    f
}

pub struct NumericCastEncoder<T, U> {
    inner: Box<dyn StreamingEncoder<U>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, U> NumericCastEncoder<T, U> {
    pub fn new(inner: Box<dyn StreamingEncoder<U>>) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, U> StreamingEncoder<T> for NumericCastEncoder<T, U>
where
    T: Copy + Into<U> + Send + Sync + 'static,
    U: 'static + Copy + Send + 'static,
{
    fn begin_stream(&self, writer: &mut dyn io::Write) -> std::io::Result<()> {
        self.inner.begin_stream(writer)
    }

    fn encode_value(&self, v: &T, writer: &mut dyn io::Write) -> std::io::Result<()> {
        let u: U = (*v).into();
        self.inner.encode_value(&u, writer)
    }

    fn end_stream(&self, writer: &mut dyn io::Write) -> std::io::Result<()> {
        self.inner.end_stream(writer)
    }
}
