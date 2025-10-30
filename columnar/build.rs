use columnar_codegen::expand_simple_columnar;
use std::{env, fs, path::PathBuf};
use syn::{Item, Path, parse_file, punctuated::Punctuated, Token};

fn main() {
    // Locate the source file we want to expand.
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let source = manifest_dir.join("src/example.rs");

    println!("cargo:rerun-if-changed={}", source.display());

    // Read and parse the file.
    let content =
        fs::read_to_string(&source).expect("Could not read src/example.rs for SimpleColumnar codegen");
    let syntax = parse_file(&content).expect("Failed to parse src/example.rs");

    // Collect all structs in this file with #[derive(SimpleColumnar)]
    let mut generated = Vec::new();
    for item in syntax.items {
        if let Item::Struct(ref s) = item {
            for attr in &s.attrs {
                if attr.path().is_ident("derive")
                    && derives_simple_columnar(attr).unwrap_or(false)
                {
                    let derive_input = syn::DeriveInput {
                        attrs: s.attrs.clone(),
                        vis: s.vis.clone(),
                        ident: s.ident.clone(),
                        generics: s.generics.clone(),
                        data: syn::Data::Struct(syn::DataStruct {
                            struct_token: s.struct_token,
                            fields: s.fields.clone(),
                            semi_token: s.semi_token,
                        }),
                    };

                    let tokens = expand_simple_columnar(&derive_input)
                        .expect("SimpleColumnar expansion failed during build script");
                    generated.push(tokens);
                }
            }
        }
    }

    let mut output = String::new();
    if generated.is_empty() {
        output.push_str("// No #[derive(SimpleColumnar)] structs found in src/example.rs\n");
    } else {
        for g in generated {
            output.push_str(&g.to_string());
            output.push('\n');
        }
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let expanded = out_dir.join("simple_columnar_expanded.rs");
    fs::write(&expanded, &output).expect("Failed to write generated SimpleColumnar output");

    // Also write a copy under columnar/generated/ for easy inspection in the repo.
    let preview_dir = manifest_dir.join("generated");
    fs::create_dir_all(&preview_dir).expect("Failed to create generated preview directory");
    let preview_file = preview_dir.join("simple_columnar_expanded.rs");
    fs::write(&preview_file, output).expect("Failed to write generated preview file");
    println!(
        "cargo:warning=SimpleColumnar preview written to {}",
        preview_file.display()
    );
}

fn derives_simple_columnar(attr: &syn::Attribute) -> syn::Result<bool> {
    let paths: Punctuated<Path, Token![,]> =
        attr.parse_args_with(Punctuated::parse_terminated)?;
    Ok(paths.iter().any(|path| path.is_ident("SimpleColumnar")))
}
