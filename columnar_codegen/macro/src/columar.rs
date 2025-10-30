use columnar_codegen::expand_columnar as expand_columnar_impl;
use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

pub(crate) fn expand_columnar(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_columnar_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
