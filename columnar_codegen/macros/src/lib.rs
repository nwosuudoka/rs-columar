use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(Columnar, attributes(columnar))]
pub fn derive_columnar(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match columnar_codegen::expand_columnar(&input, None) {
        Ok(tokens) => TokenStream::from(tokens),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_derive(SimpleColumnar, attributes(columnar))]
pub fn derive_simple_columnar(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match columnar_codegen::expand_simple_columnar(&input, None) {
        Ok(tokens) => TokenStream::from(tokens),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_derive(StreamingColumnar, attributes(columnar))]
pub fn derive_streaming_columnar(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match columnar_codegen::expand_streaming_columnar(&input, None) {
        Ok(tokens) => TokenStream::from(tokens),
        Err(err) => err.to_compile_error().into(),
    }
}
