use proc_macro_crate::{FoundCrate, crate_name};
use syn::{Path, parse_quote};

pub fn runtime_path() -> syn::Result<Path> {
    // Try using proc_macro_crate — works in proc macro context
    if let Ok(result) = crate_name("columnar") {
        return Ok(match result {
            FoundCrate::Itself => parse_quote!(crate),
            FoundCrate::Name(name) => syn::parse_str(&name)
                .map_err(|err| syn::Error::new(proc_macro2::Span::call_site(), err))?,
        });
    }
    Ok(parse_quote!(crate))
}

// Fallback: when running outside macro context (like builder)
// Just assume we’re generating into the same crate

// use proc_macro_crate::{FoundCrate, crate_name};
// use syn::{Path, parse_quote};

// pub fn runtime_path() -> syn::Result<Path> {
//     Ok(match crate_name("columnar") {
//         Ok(FoundCrate::Itself) => parse_quote!(crate),
//         Ok(FoundCrate::Name(name)) => syn::parse_str(&name)
//             .map_err(|err| syn::Error::new(proc_macro2::Span::call_site(), err))?,
//         Err(_) => parse_quote!(columnar),
//     })
// }

// pub fn runtime_path_2() -> syn::Result<Path> {
//     Ok(match crate_name("columnar") {
//         Ok(FoundCrate::Itself) => parse_quote!(crate),
//         Ok(FoundCrate::Name(name)) => syn::parse_str(&name)
//             .map_err(|err| syn::Error::new(proc_macro2::Span::call_site(), err))?,
//         Err(_) => parse_quote!(columnar),
//     })
// }
