pub mod attr;
pub mod fields;
mod pathing;
mod columnar;
mod simple;

use proc_macro2::TokenStream;
use syn::DeriveInput;

pub fn expand_columnar(input: &DeriveInput) -> syn::Result<TokenStream> {
    columnar::expand(input)
}

pub fn expand_simple_columnar(input: &DeriveInput) -> syn::Result<TokenStream> {
    simple::expand(input)
}
