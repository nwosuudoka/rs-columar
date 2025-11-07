use crate::{attr, generate, pathing};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, Result};

pub fn expand(
    input: &DeriveInput,
    maybe_quality_path: Option<proc_macro2::TokenStream>,
) -> Result<TokenStream> {
    let rt = pathing::runtime_path().unwrap();
    let row_indent = &input.ident;
    let vis = input.vis.clone();
    let columns_ident = format_ident!("{}VecColumns", row_indent);

    let fields = match &input.data {
        Data::Struct(ds) => match &ds.fields {
            // Fields::Named(n) => n.named,
            Fields::Named(named) => named.named.iter().cloned().collect::<Vec<_>>(),
            _ => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "SimpleColumnar requires structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "SimpleColumnar can only be derived for structs",
            ));
        }
    };

    let mut specs = Vec::<generate::FieldSpec>::new();
    for f in fields {
        let field_ident = f.ident.unwrap();
        let fattrs = attr::parse_field_attrs(&f.attrs)?;
        let field_ty = f.ty;
        let col_name = fattrs
            .rename
            .clone()
            .unwrap_or_else(|| field_ident.to_string());
        let column_ident = format_ident!("{}", col_name);
        specs.push(generate::FieldSpec {
            field_ident: field_ident.clone(),
            field_ty,
            column_ident: column_ident.into(),
            fattrs,
        });
    }

    let backend_ty_for = |fs: &generate::FieldSpec| {
        let ty = &fs.field_ty;
        // quote! {::std::vec::Vec<#ty>}
        quote! { #rt::VecColumn<#ty> }
    };

    let cols_struct = generate::make_column_struct(
        &vis,
        &columns_ident,
        &specs,
        &backend_ty_for,
        &["Debug", "Default"],
    );

    let push_body = generate::push_impl_body(&specs);
    let merge_body = generate::merge_impl_body(&specs);

    let row_path = maybe_quality_path.unwrap_or_else(|| quote! { #row_indent});
    let impl_bundle = quote! {
        impl #rt::SimpleColumnBundle<#row_path> for #columns_ident {
            fn push(&mut self, row: &#row_path) {
                #push_body
            }

            fn merge(&mut self, other: Self) {
                #merge_body
            }
        }
    };

    let impl_row = quote! {
        impl #rt::SimpleColumnar for #row_path {
            type Columns = #columns_ident;
        }
    };

    let filtered_push_body = generate::push_with_config_body(&specs);
    let impl_filtered = quote! {
        impl #rt::FilteredPush<#row_path> for #columns_ident {
            fn push_with_config(&mut self, row: &#row_path, cfg: &#rt::PushConfig) {
                #filtered_push_body
            }
        }
    };

    Ok(quote! {
        #cols_struct
        #impl_bundle
        #impl_row
        #impl_filtered
    })
}
