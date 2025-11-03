use crate::{attr, generate, pathing};
use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote};
use syn::{Data, DeriveInput, Fields, Result};

pub fn expand(
    input: &DeriveInput,
    maybe_quality_path: Option<proc_macro2::TokenStream>,
) -> Result<TokenStream> {
    let rt = pathing::runtime_path().unwrap();
    let sattr = attr::parse_struct_attrs(&input.attrs)?;
    let row_indent = &input.ident;
    let vis = input.vis.clone();
    let columns_ident = format_ident!("{}StreamColumn", row_indent);

    let fields = match &input.data {
        Data::Struct(ds) => match &ds.fields {
            Fields::Named(named) => named.named.iter().cloned().collect::<Vec<_>>(),
            _ => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "StreamColumnar requires structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "StreamColumnar can only be derived for structs",
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
        quote! { #rt::StreamColumn<#ty> }
    };

    let cols_struct =
        generate::make_column_struct(&vis, &columns_ident, &specs, &backend_ty_for, &["Debug"]);

    // let base_path = sattr.base_path.unwrap_or(".".to_string());
    let inits = specs.iter().filter(|f| !f.fattrs.skip).map(|f| {
        let ci = &f.column_ident;
        let enc = &f.fattrs.encoder.as_deref().unwrap_or("byte_size");
        let enc_expr = match enc {
            &"bitpack" => quote! {
                #rt::encoders::BitpackEncoder::new(),
            },
            &"delta" => quote! {
                #rt::encoders::DeltaEncoder::new(),
            },
            _ => quote! {
                #rt::encoders::ByteSizeEncoding::new(),
            },
        };

        let path_expr = f
            .fattrs
            .path
            .as_deref()
            .map(|p| quote! {#p})
            .unwrap_or_else(|| {
                // let filename = format!("{}.bin", ci);
                // quote! ( #filename )
                let filename = format!("{}.bin", ci.to_token_stream());
                if let Some(base) = &sattr.base_path {
                    let joined = format!("{}/{}", base.trim_end_matches('/'), filename);
                    quote! { #joined }
                } else {
                    quote! { #filename }
                }
            });

        quote! {
            #ci: #rt::StreamColumn::new(#path_expr, Box::new(#enc_expr)).unwrap(),
        }
    });

    let push_body = {
        let stmts = specs.iter().filter(|f| !f.fattrs.skip).map(|f| {
            let fi = &f.field_ident;
            let ci = &f.column_ident;
            quote! {
                self.#ci.push(&row.#fi);
            }
        });
        quote! { #(#stmts)* }
    };
    let merge_body = {
        let stmts = specs.iter().filter(|f| !f.fattrs.skip).map(|f| {
            let ci = &f.column_ident;
            quote! {
                self.#ci.merge(other.#ci);
            }
        });

        quote! {
            #(#stmts)*
        }
    };

    let impl_default = quote! {
        impl Default for #columns_ident {
            fn default() -> Self {
                Self {
                    #(#inits)*
                }
            }
        }
    };

    let row_path = maybe_quality_path.unwrap_or_else(|| quote! { #row_indent});
    let impl_bundle = quote! {
        impl #rt::StreamingColumnBundle<#row_indent> for #columns_ident {
            fn push(&mut self, row: &#row_path) {
                #push_body
            }

            fn merge(&mut self, other: Self) {
                #merge_body
            }
        }
    };

    let impl_row = quote! {
        impl #rt::StreamingColumnar for #row_path {
            type Columns = #columns_ident;
        }
    };

    Ok(quote! {
        #cols_struct
        #impl_default
        #impl_bundle
        #impl_row
    })
}
