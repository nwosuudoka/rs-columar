use crate::{attr, fields::FieldSpec, pathing};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{spanned::Spanned, Data, DeriveInput, Fields, Result};

pub fn expand(input: &DeriveInput) -> Result<TokenStream> {
    let runtime = pathing::runtime_path()?;

    let row_ident = &input.ident;
    let vis = &input.vis;
    let columns_ident = format_ident!("{}VecColumns", row_ident);

    let fields = match &input.data {
        Data::Struct(ds) => match &ds.fields {
            Fields::Named(named) => named.named.iter().cloned().collect::<Vec<_>>(),
            _ => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "SimpleColumnar requires structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "SimpleColumnar can only be derived for structs",
            ))
        }
    };

    let mut specs = Vec::new();
    for field in fields {
        let field_ident = field.ident.clone().ok_or_else(|| {
            syn::Error::new(field.span(), "SimpleColumnar requires structs with named fields")
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

    let column_fields = specs.iter().filter(|spec| !spec.attrs.skip).map(|spec| {
        let column_ident = &spec.column_ident;
        let field_ty = &spec.field_ty;
        quote! { pub #column_ident: #runtime::VecColumn<#field_ty>, }
    });

    let push_body = specs.iter().filter(|spec| !spec.attrs.skip).map(|spec| {
        let field_ident = &spec.field_ident;
        let column_ident = &spec.column_ident;
        quote! { self.#column_ident.push(&row.#field_ident); }
    });

    let merge_body = specs.iter().filter(|spec| !spec.attrs.skip).map(|spec| {
        let column_ident = &spec.column_ident;
        quote! { self.#column_ident.merge(other.#column_ident); }
    });

    Ok(quote! {
        #[derive(Default, Debug)]
        #vis struct #columns_ident {
            #(#column_fields)*
        }

        impl #runtime::ColumnBundle<#row_ident> for #columns_ident {
            fn push(&mut self, row: &#row_ident) {
                #(#push_body)*
            }

            fn merge(&mut self, other: Self) {
                #(#merge_body)*
            }

            fn set_chunk_size(&mut self, _: usize) {}
        }

        impl #runtime::Columnar for #row_ident {
            type Columns = #columns_ident;
        }
    })
}
