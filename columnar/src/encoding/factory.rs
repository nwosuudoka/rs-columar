use std::{any::TypeId, collections::HashMap};

use crate::encoding::streaming::StreamingEncoder;

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
