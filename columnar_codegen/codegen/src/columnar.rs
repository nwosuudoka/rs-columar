use crate::{attr, fields::FieldSpec, pathing};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{spanned::Spanned, Data, DeriveInput, Fields, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let runtime = pathing::runtime_path()?;
    let struct_attrs = attr::parse_struct_attrs(&input.attrs)?;

    let row_ident = &input.ident;
    let vis = &input.vis;
    let columns_ident = format_ident!("{}Columns", row_ident);

    let fields = match &input.data {
        Data::Struct(ds) => match &ds.fields {
            Fields::Named(named) => named.named.iter().cloned().collect::<Vec<_>>(),
            _ => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "Columnar requires structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "Columnar can only be derived for structs",
            ))
        }
    };

    let mut specs = Vec::new();
    for field in fields {
        let field_ident = field.ident.clone().ok_or_else(|| {
            syn::Error::new(field.span(), "Columnar requires structs with named fields")
        })?;
        let field_ty = field.ty.clone();
        let attrs = attr::parse_field_attrs(&field.attrs)?;
        let column_ident = format_ident!("{}", attrs.rename.clone().unwrap_or_else(|| field_ident.to_string()));

        specs.push(FieldSpec {
            field_ident,
            field_ty,
            column_ident,
            attrs,
        });
    }

    let column_fields = specs
        .iter()
        .filter(|spec| !spec.attrs.skip)
        .map(|spec| {
            let column_ident = &spec.column_ident;
            let field_ty = &spec.field_ty;
            let runtime = runtime.clone();
            quote! { pub #column_ident: #runtime::Column<#field_ty>, }
        })
        .collect::<Vec<_>>();

    let push_body = specs
        .iter()
        .filter(|spec| !spec.attrs.skip)
        .map(|spec| {
            let field_ident = &spec.field_ident;
            let column_ident = &spec.column_ident;
            quote! { self.#column_ident.push(&row.#field_ident); }
        })
        .collect::<Vec<_>>();

    let merge_body = specs
        .iter()
        .filter(|spec| !spec.attrs.skip)
        .map(|spec| {
            let column_ident = &spec.column_ident;
            quote! { self.#column_ident.extend_from(&other.#column_ident); }
        })
        .collect::<Vec<_>>();

    let set_chunk_body = specs
        .iter()
        .filter(|spec| !spec.attrs.skip)
        .map(|spec| {
            let column_ident = &spec.column_ident;
            quote! { self.#column_ident = std::mem::take(&mut self.#column_ident).with_chunk_size(n); }
        })
        .collect::<Vec<_>>();

    let chunk_size_impl = if let Some(chunk_size) = struct_attrs.chunk_size {
        let init_fields = specs
            .iter()
            .filter(|spec| !spec.attrs.skip)
            .map(|spec| {
                let column_ident = &spec.column_ident;
                let runtime = runtime.clone();
                quote! { #column_ident: #runtime::Column::default().with_chunk_size(#chunk_size), }
            })
            .collect::<Vec<_>>();

        quote! {
            impl Default for #columns_ident {
                fn default() -> Self {
                    Self { #(#init_fields)* }
                }
            }
        }
    } else {
        quote! { #[derive(Default, Debug)] #vis struct #columns_ident { #(#column_fields)* } }
    };

    let struct_decl_if_needed = if struct_attrs.chunk_size.is_some() {
        quote! { #[derive(Debug)] #vis struct #columns_ident { #(#column_fields)* } }
    } else {
        quote! {}
    };

    Ok(quote! {
        #struct_decl_if_needed
        #chunk_size_impl

        impl #runtime::ColumnBundle<#row_ident> for #columns_ident {
            fn push(&mut self, row: &#row_ident) {
                #(#push_body)*
            }

            fn merge(&mut self, other: Self) {
                #(#merge_body)*
            }

            fn set_chunk_size(&mut self, n: usize) {
                #(#set_chunk_body)*
            }
        }

        impl #runtime::Columnar for #row_ident {
            type Columns = #columns_ident;
        }
    })
}
