use crate::{
    attr::{self, StructAttrs},
    generate, pathing,
};
use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote};
use syn::{Data, DeriveInput, Field, Fields, Result};

pub fn expand(
    input: &DeriveInput,
    maybe_quality_path: Option<proc_macro2::TokenStream>,
) -> Result<TokenStream> {
    let rt = pathing::runtime_path().unwrap();
    let sattr = attr::parse_struct_attrs(&input.attrs)?;
    let row_ident = &input.ident;
    let vis = input.vis.clone();
    let columns_ident = format_ident!("{}StreamColumn", row_ident);

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

    // let mut specs = Vec::<generate::FieldSpec>::new();
    // for f in fields {
    //     let field_ident = f.ident.unwrap();
    //     let fattrs = attr::parse_field_attrs(&f.attrs)?;
    //     let field_ty = f.ty;
    //     let col_name = fattrs
    //         .rename
    //         .clone()
    //         .unwrap_or_else(|| field_ident.to_string());
    //     let column_ident = format_ident!("{}", col_name);
    //     specs.push(generate::FieldSpec {
    //         field_ident: field_ident.clone(),
    //         field_ty,
    //         column_ident: column_ident.into(),
    //         fattrs,
    //     });
    // }
    let specs = get_specs(&fields);

    let backend_ty_for = |fs: &generate::FieldSpec| {
        let ty = &fs.field_ty;
        quote! { #rt::StreamColumn<#ty> }
    };

    let cols_struct =
        generate::make_column_struct(&vis, &columns_ident, &specs, &backend_ty_for, &["Debug"]);

    // 3️⃣ Encoder initialization with optional pool injection
    let inits = specs.iter().filter(|f| !f.fattrs.skip).map(|f| {
        let ci = &f.column_ident;
        let ty = &f.field_ty;
        let encoder_name = f.fattrs.encoder.as_deref().unwrap_or("bitpack");

        // Determine if encoder expects a pool argument
        let (encoder_expr, needs_pool) = match encoder_name {
            "bitpack" => (
                quote! { #rt::encoding::BitpackStreamWriter::<#ty>::new },
                true,
            ),
            "string" => (quote! { #rt::encoding::StringStreamEncoder::new }, true),
            "delta" => (
                quote! { #rt::encoding::DeltaStreamEncoder::<#ty>::new },
                false,
            ),
            _ => (quote! { compile_error!("Unknown encoder type"); }, false),
        };

        // Directory-style path: StructName/field.bin
        let struct_name = row_ident.to_string();
        let field_name = ci.to_token_stream().to_string().replace(' ', "");
        let rel_path = format!("{}/{}.bin", struct_name, field_name);

        let path_expr = if let Some(base) = &sattr.base_path {
            let joined = format!("{}/{}", base.trim_end_matches('/'), rel_path);
            quote! { #joined }
        } else {
            quote! { #rel_path }
        };

        // let index_expr = if f.fattrs.index {
        //     let index_type = f.fattrs.index_type.as_deref().unwrap();
        //     let rel_index_path = format!("{}/{}.idx", struct_name, field_name);
        //     let index_path_expr = if let Some(index_path) = &f.fattrs.index_path {
        //         quote! { #index_path }
        //     } else if let Some(base) = &sattr.base_path {
        //         let joined = format!("{}/{}", base.trim_end_matches('/'), rel_index_path);
        //         quote! { #joined }
        //     } else {
        //         quote! { #rel_index_path }
        //     };

        //     match index_type {
        //         "doc_index" => quote! {
        //             Some(Box::new(#rt::indexing::DocIndex::new(#index_path_expr)) as Box<dyn #rt::FieldIndex<#ty>>)
        //         },
        //         "categorical" => quote! {
        //             Some(Box::new(#rt::indexing::Categorial::new(#index_path_expr)) as Box<dyn #rt::FieldIndex<#ty>>)
        //         },
        //         _ =>
        //             quote! {
        //                 compile_error!("Unknown index type")
        //              }
        //     }
        // } else {
        //     quote! { None }
        // };
        let index_expr = get_index_expr(f, &struct_name, &field_name, sattr.clone(), rt.clone());

        // Conditionally add pool
        if needs_pool {
            quote! {
                #ci: #rt::StreamColumn::new(
                    #path_expr,
                    pool.clone(),
                    Box::new(#encoder_expr(pool.clone())),
                    #index_expr
                ).unwrap(),
            }
        } else {
            quote! {
                #ci: #rt::StreamColumn::new(
                    #path_expr,
                    #rt::SmartBufferPool::default(),
                    Box::new(#encoder_expr()),
                    #index_expr
                ).unwrap(),
            }
        }
    });

    let push_body = generate::push_impl_body_stream(&specs);
    let merge_body = generate::merge_impl_body(&specs);

    let impl_default = quote! {
        impl #columns_ident {
            fn with_pool(pool: #rt::SmartBufferPool) -> Self {
                Self {
                    #(#inits)*
                }
            }
        }

        impl Default for #columns_ident {
            fn default() -> Self {
                let pool = #rt::SmartBufferPool::new(64 * 1024);
                Self::with_pool(pool)
            }
        }
    };

    let row_path = maybe_quality_path.unwrap_or_else(|| quote! { #row_ident});
    let impl_bundle = quote! {
        impl #rt::StreamingColumnBundle<#row_path> for #columns_ident {
            fn push(&mut self, row: &#row_path) -> std::io::Result<()> {
                #push_body
                Ok(())
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

    let filtered_push_body = generate::push_with_config_body_stream(&specs);
    let impl_filtered = quote! {
        impl #rt::FilteredPush<#row_path> for #columns_ident {
            fn push_with_config(&mut self, row: &#row_path, cfg: &#rt::PushConfig) -> io::Result<()> {
                #filtered_push_body
                Ok(())
            }
        }
    };

    Ok(quote! {
        #cols_struct
        #impl_default
        #impl_bundle
        #impl_row
        #impl_filtered
    })
}

fn get_specs(fields: &[Field]) -> Vec<generate::FieldSpec> {
    fields
        .iter()
        .map(|f| {
            let field_ident = f.ident.clone().unwrap();
            let fattrs = attr::parse_field_attrs(&f.attrs).unwrap();
            let field_ty = f.ty.clone();
            let col_name = fattrs
                .rename
                .clone()
                .unwrap_or_else(|| field_ident.to_string());
            let column_ident = format_ident!("{}", col_name);
            generate::FieldSpec {
                field_ident: field_ident.clone(),
                field_ty,
                column_ident: column_ident.into(),
                fattrs,
            }
        })
        .collect::<Vec<_>>()
}

fn get_index_expr(
    f: &generate::FieldSpec,
    struct_name: &str,
    field_name: &str,
    sattr: StructAttrs,
    rt: syn::Path,
) -> TokenStream {
    if f.fattrs.index {
        let index_type = f.fattrs.index_type.as_deref().unwrap();
        let rel_index_path = format!("{}/{}.idx", struct_name, field_name);
        let index_path_expr = if let Some(index_path) = &f.fattrs.index_path {
            quote! { #index_path }
        } else if let Some(base) = &sattr.base_path {
            let joined = format!("{}/{}", base.trim_end_matches('/'), rel_index_path);
            quote! { #joined }
        } else {
            quote! { #rel_index_path }
        };

        let ty = &f.field_ty;

        match index_type {
            "doc_index" => quote! {
                Some(Box::new(#rt::indexing::DocIndex::new(#index_path_expr)) as Box<dyn #rt::FieldIndex<#ty>>)
            },
            "categorical" => quote! {
                Some(Box::new(#rt::indexing::Categorial::new(#index_path_expr)) as Box<dyn #rt::FieldIndex<#ty>>)
            },
            _ => quote! {
            compile_error!("Unknown index type")
            },
        }
    } else {
        quote! { None }
    }
}
