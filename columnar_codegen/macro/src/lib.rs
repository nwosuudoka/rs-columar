use proc_macro::TokenStream;

mod columar;
mod simple_columnar;

#[proc_macro_derive(Columnar, attributes(columnar))]
pub fn derive_columnar(input: TokenStream) -> TokenStream {
    columar::expand_columnar(input)
}

#[proc_macro_derive(SimpleColumnar, attributes(columnar))]
pub fn derive_simple_columnar(input: TokenStream) -> TokenStream {
    simple_columnar::expand_simple_columnar(input)
}
