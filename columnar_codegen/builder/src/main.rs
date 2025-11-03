use columnar_codegen::{expand_columnar, expand_simple_columnar, expand_streaming_columnar};
use quote::{format_ident, quote};
use std::fs;
use std::path::{Path, PathBuf};
use syn::{Attribute, DataStruct, DeriveInput, Fields, Ident, Item, ItemStruct, parse_file};

fn main() -> syn::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let streaming_mode = args.iter().any(|a| a == "--stream");
    println!("streaming mode: {}", streaming_mode);

    let (input_dir, output_dir) = columnar_paths();
    fs::create_dir_all(&output_dir).unwrap();

    println!(
        "creating output dir {}\nfrom input {}",
        output_dir.display(),
        input_dir.display(),
    );

    for entry in fs::read_dir(input_dir).unwrap() {
        let file = entry.unwrap().path();
        if file.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }

        let src = fs::read_to_string(&file).unwrap();
        let parsed = parse_file(&src).unwrap();
        for item in parsed.items {
            if let Item::Struct(s) = item {
                let has_stream = has_derive(&s, "StreamingColumnar");
                let has_simple = has_derive(&s, "SimpleColumnar");
                let has_columnar = has_derive(&s, "Columnar");

                let derive_input = item_struct_to_derive_input(&s);

                let struct_name = &s.ident;
                let module_path_idents = module_path_from_file(&file);
                let struct_path = quote! { crate::#(#module_path_idents::)*#struct_name };

                let generated = if streaming_mode && has_stream {
                    expand_streaming_columnar(&derive_input, Some(struct_path))?
                } else if !streaming_mode && has_simple {
                    expand_simple_columnar(&derive_input, Some(struct_path))?
                } else if !streaming_mode && has_columnar {
                    // continue
                    expand_columnar(&derive_input, Some(struct_path))?
                } else {
                    continue;
                };

                let name = s.ident.to_string();
                let mode = {
                    if streaming_mode {
                        "stream"
                    } else if has_simple {
                        "vec"
                    } else {
                        "vec_chunks"
                    }
                };
                let out_path = output_dir.join(format!("{}_{}.rs", name.to_lowercase(), mode));

                fs::write(&out_path, generated.to_string()).unwrap();
                format_with_rustfmt(&out_path);
                println!("Generated {}", out_path.display());
            }
        }
    }
    Ok(())
}

fn has_derive(s: &ItemStruct, name: &str) -> bool {
    s.attrs.iter().any(|attr| is_derive_with(attr, name))
}

fn is_derive_with(attr: &Attribute, wanted: &str) -> bool {
    if !attr.path().is_ident("derive") {
        return false;
    }

    let mut found = false;
    let _ = attr.parse_nested_meta(|meta| {
        if meta.path.is_ident(wanted) {
            found = true;
        }
        Ok(())
    });
    found
}
fn item_struct_to_derive_input(s: &ItemStruct) -> DeriveInput {
    let fields = match &s.fields {
        Fields::Named(named) => Fields::Named(named.clone()),
        Fields::Unnamed(unnamed) => Fields::Unnamed(unnamed.clone()),
        Fields::Unit => Fields::Unit,
    };
    DeriveInput {
        attrs: s.attrs.clone(),
        vis: s.vis.clone(),
        ident: s.ident.clone(),
        generics: s.generics.clone(),
        data: syn::Data::Struct(DataStruct {
            struct_token: s.struct_token,
            fields,
            semi_token: s.semi_token,
        }),
    }
}

fn columnar_paths() -> (PathBuf, PathBuf) {
    // this points to: dataencoder/columnar_codegen/builder
    let builder_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // go up two levels: columnar_codegen/builder → columnar_codegen → dataencoder
    // then into columnar/
    let columnar_dir = builder_dir
        .parent() // ../columnar_codegen
        .and_then(|p| p.parent()) // ../dataencoder
        .map(|p| p.join("columnar"))
        .expect("Cannot locate columnar/ directory");

    let input_dir = columnar_dir.join("src/models");
    let output_dir = columnar_dir.join("src/generated");
    (input_dir, output_dir)
}

fn format_with_rustfmt(path: &Path) {
    let Some(path_str) = path.to_str() else {
        eprintln!("invalid path: {}", path.display());
        return;
    };

    match std::process::Command::new("rustfmt")
        .args(["--edition", "2024", path_str])
        .status()
    {
        Ok(status) if status.success() => {
            println!("formatted {}", path.display());
        }
        Ok(_) => {
            eprintln!(
                "rustfmt exited with a non-zero status for {}",
                path.display()
            );
        }
        Err(e) => {
            eprintln!("failed to run rustfmt {e}");
        }
    }
}

fn module_path_from_file(file: &Path) -> Vec<Ident> {
    fn to_snake_case(name: &str) -> String {
        let mut snake = String::new();
        let mut chars = name.chars().peekable();
        let mut has_prev = false;

        while let Some(ch) = chars.next() {
            if ch.is_uppercase() {
                if has_prev
                    && let Some(next) = chars.peek()
                    && (next.is_lowercase() || next.is_numeric())
                {
                    snake.push('_');
                }
                for lower in ch.to_lowercase() {
                    snake.push(lower);
                }
                has_prev = true;
            } else if matches!(ch, '-' | ' ') {
                if has_prev {
                    snake.push('_');
                }
                has_prev = false;
            } else {
                snake.push(ch);
                has_prev = ch.is_alphanumeric();
            }
        }
        snake
    }

    // let relative = file.strip_prefix("src").unwrap_or(file);
    let (input_dir, _) = columnar_paths();
    let relative = file
        .strip_prefix(input_dir.parent().unwrap())
        .unwrap_or(file);

    relative
        .iter()
        .filter_map(|component| {
            let raw = component.to_str()?;
            if raw == "mod.rs" {
                return None;
            }
            let without_ext = raw.strip_suffix(".rs").unwrap_or(raw);
            if without_ext.is_empty() {
                None
            } else {
                Some(format_ident!("{}", to_snake_case(without_ext)))
            }
        })
        .collect()
}
