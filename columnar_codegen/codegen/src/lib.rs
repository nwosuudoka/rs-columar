use proc_macro2::TokenStream;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Fields, LitInt, LitStr, Result, Type, parse_quote, spanned::Spanned,
};

pub fn expand_columnar(input: &DeriveInput) -> Result<TokenStream> {
    let runtime_crate_path = resolve_runtime_crate_path()?;

    let row_ident = &input.ident;
    let vis = &input.vis;
    let columns_ident = format_ident!("{}Columns", row_ident);

    // Parse struct-level attributes (e.g., #[columnar(chunk_size = 500_000)])
    let mut struct_chunk_size: Option<usize> = None;
    for attr in &input.attrs {
        if attr.path().is_ident("columnar") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("chunk_size") {
                    let lit: LitInt = meta.value()?.parse()?;
                    let value = lit.base10_parse::<usize>()?;
                    if struct_chunk_size.replace(value).is_some() {
                        return Err(meta.error("duplicate columnar(chunk_size) attribute"));
                    }
                    Ok(())
                } else {
                    Err(meta.error("unsupported columnar attribute on struct"))
                }
            })?;
        }
    }

    let fields = collect_named_fields(input, "Columnar")?;

    struct FieldSpec {
        field_ident: syn::Ident,
        field_ty: Type,
        column_ident: syn::Ident,
        skip: bool,
    }

    let mut specs: Vec<FieldSpec> = Vec::new();
    for field in fields {
        let field_ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new(field.span(), "Columnar requires named fields"))?;
        let field_ty = field.ty.clone();
        let (rename, skip) = parse_field_attrs(&field.attrs)?;

        let col_name = rename.unwrap_or_else(|| field_ident.to_string());
        let column_ident = format_ident!("{}", col_name);

        specs.push(FieldSpec {
            field_ident,
            field_ty,
            column_ident,
            skip,
        });
    }

    let col_struct_fields: Vec<_> = specs
        .iter()
        .filter(|s| !s.skip)
        .map(|spec| {
            let column_ident = &spec.column_ident;
            let field_ty = &spec.field_ty;
            let runtime_crate_path = runtime_crate_path.clone();
            quote! { pub #column_ident: #runtime_crate_path::Column<#field_ty>, }
        })
        .collect();

    let push_body = specs.iter().filter(|s| !s.skip).map(|spec| {
        let column_ident = &spec.column_ident;
        let field_ident = &spec.field_ident;
        quote! { self.#column_ident.push(&row.#field_ident); }
    });

    let set_chunk_size_body = specs.iter().filter(|s| !s.skip).map(|spec| {
        let column_ident = &spec.column_ident;
        quote! { self.#column_ident = std::mem::take(&mut self.#column_ident).with_chunk_size(n); }
    });

    let merge_body = specs.iter().filter(|s| !s.skip).map(|spec| {
        let column_ident = &spec.column_ident;
        quote! { self.#column_ident.extend_from(&other.#column_ident); }
    });

    let maybe_struct_chunk_init = if let Some(n) = struct_chunk_size {
        let inits = specs.iter().filter(|s| !s.skip).map(|spec| {
            let column_ident = &spec.column_ident;
            let runtime_crate_path = runtime_crate_path.clone();
            quote! { #column_ident: #runtime_crate_path::Column::default().with_chunk_size(#n), }
        });
        quote! {
            impl Default for #columns_ident {
                fn default() -> Self {
                    Self { #(#inits)* }
                }
            }
        }
    } else {
        quote! { #[derive(Default)] #vis struct #columns_ident { #(#col_struct_fields)* } }
    };

    let struct_decl_if_needed = if struct_chunk_size.is_some() {
        quote! { #vis struct #columns_ident { #(#col_struct_fields)* } }
    } else {
        quote! {}
    };

    Ok(quote! {
        #struct_decl_if_needed
        #maybe_struct_chunk_init

        impl #runtime_crate_path::ColumnStorageBundle<#row_ident> for #columns_ident {
            fn push(&mut self, row: &#row_ident) {
                #(#push_body)*
            }

            fn merge(&mut self, other: Self) {
                #(#merge_body)*
            }

            fn set_chunk_size(&mut self, n: usize) {
                #(#set_chunk_size_body)*
            }
        }

        impl #runtime_crate_path::Columnar for #row_ident {
            type Columns = #columns_ident;
        }
    })
}

pub fn expand_simple_columnar(input: &DeriveInput) -> Result<TokenStream> {
    let runtime_crate_path = resolve_runtime_crate_path()?;

    let row_ident = &input.ident;
    let vis = &input.vis;
    let columns_ident = format_ident!("{}VecColumns", row_ident);

    let fields = collect_named_fields(input, "SimpleColumnar")?;

    struct FieldSpec {
        field_ident: syn::Ident,
        field_ty: Type,
        column_ident: syn::Ident,
        skip: bool,
    }

    let mut specs: Vec<FieldSpec> = Vec::new();
    for field in fields {
        let field_ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new(field.span(), "SimpleColumnar requires named fields"))?;
        let field_ty = field.ty.clone();
        let (rename, skip) = parse_field_attrs(&field.attrs)?;

        let col_name = rename.unwrap_or_else(|| field_ident.to_string());
        let column_ident = format_ident!("{}", col_name);

        specs.push(FieldSpec {
            field_ident,
            field_ty,
            column_ident,
            skip,
        });
    }

    let col_struct_fields: Vec<_> = specs
        .iter()
        .filter(|s| !s.skip)
        .map(|spec| {
            let column_ident = &spec.column_ident;
            let field_ty = &spec.field_ty;
            quote! { pub #column_ident: Vec<#field_ty>, }
        })
        .collect();

    let push_body = specs.iter().filter(|s| !s.skip).map(|spec| {
        let column_ident = &spec.column_ident;
        let field_ident = &spec.field_ident;
        quote! { self.#column_ident.push(row.#field_ident.clone()); }
    });

    let merge_body = specs.iter().filter(|s| !s.skip).map(|spec| {
        let column_ident = &spec.column_ident;
        quote! { self.#column_ident.extend(other.#column_ident); }
    });

    Ok(quote! {
        #[derive(Default)]
        #vis struct #columns_ident {
            #(#col_struct_fields)*
        }

        impl #runtime_crate_path::ColumnStorageBundle<#row_ident> for #columns_ident {
            fn push(&mut self, row: &#row_ident) {
                #(#push_body)*
            }

            fn merge(&mut self, other: Self) {
                #(#merge_body)*
            }

            fn set_chunk_size(&mut self, _: usize) {}
        }

        impl #runtime_crate_path::Columnar for #row_ident {
            type Columns = #columns_ident;
        }
    })
}

fn resolve_runtime_crate_path() -> Result<syn::Path> {
    Ok(match crate_name("columnar") {
        Ok(FoundCrate::Itself) => parse_quote!(crate),
        Ok(FoundCrate::Name(name)) => syn::parse_str(&name)
            .map_err(|err| syn::Error::new(proc_macro2::Span::call_site(), err))?,
        Err(_) => parse_quote!(columnar),
    })
}

fn collect_named_fields(input: &DeriveInput, macro_name: &str) -> Result<Vec<syn::Field>> {
    match &input.data {
        Data::Struct(ds) => match &ds.fields {
            Fields::Named(named) => Ok(named.named.iter().cloned().collect()),
            _ => Err(syn::Error::new_spanned(
                &input.ident,
                format!("{macro_name} requires structs with named fields"),
            )),
        },
        _ => Err(syn::Error::new_spanned(
            &input.ident,
            format!("{macro_name} can only be derived for structs"),
        )),
    }
}

fn parse_field_attrs(attrs: &[Attribute]) -> Result<(Option<String>, bool)> {
    let mut rename: Option<String> = None;
    let mut skip = false;
    for attr in attrs {
        if attr.path().is_ident("columnar") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    let lit: LitStr = meta.value()?.parse()?;
                    if rename.replace(lit.value()).is_some() {
                        return Err(meta.error("duplicate columnar(rename) attribute"));
                    }
                    Ok(())
                } else if meta.path.is_ident("skip") {
                    if !meta.input.is_empty() {
                        return Err(meta.error("columnar(skip) does not take a value"));
                    }
                    skip = true;
                    Ok(())
                } else {
                    Err(meta.error("unsupported columnar attribute on field"))
                }
            })?;
        }
    }
    Ok((rename, skip))
}
