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

/// Derive that only *declares* the `columnar` helper attribute so the
/// compiler accepts `#[columnar(...)]` on the struct and fields.
/// It generates **no code** and therefore won't conflict with builder output.
#[proc_macro_derive(ColumnarAttrs, attributes(columnar))]
pub fn derive_columnar_attrs(input: TokenStream) -> TokenStream {
    let _ = parse_macro_input!(input as DeriveInput);
    TokenStream::new()
}
