use proc_macro_crate::{crate_name, FoundCrate};
use syn::{parse_quote, Path};

pub fn runtime_path() -> syn::Result<Path> {
    Ok(match crate_name("columnar") {
        Ok(FoundCrate::Itself) => parse_quote!(crate),
        Ok(FoundCrate::Name(name)) => syn::parse_str(&name)
            .map_err(|err| syn::Error::new(proc_macro2::Span::call_site(), err))?,
        Err(_) => parse_quote!(columnar),
    })
}
