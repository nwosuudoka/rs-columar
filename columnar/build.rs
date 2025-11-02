use proc_macro2::Ident;
use quote::{format_ident, quote};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use syn::{Item, ItemStruct, parse_file};

const INPUT_DIR: &str = "src/models";
const OUTPUT_DIR: &str = "src/generated";

fn main() {
    fs::create_dir_all(OUTPUT_DIR).unwrap();

    // Collect all model files
    let model_files: Vec<PathBuf> = fs::read_dir(INPUT_DIR)
        .unwrap()
        .filter_map(|e| {
            let p = e.ok()?.path();
            if p.extension().and_then(|s| s.to_str()) == Some("rs") {
                Some(p)
            } else {
                None
            }
        })
        .collect();

    for file in &model_files {
        let src = fs::read_to_string(file).unwrap();
        let parsed = parse_file(&src).unwrap();

        // find all struct definitions in the file
        for item in parsed.items {
            if let Item::Struct(s) = item {
                generate_columnar_for_struct(&s, file);
            }
        }

        println!("cargo:rerun-if-changed={}", file.display());
    }

    // Generate mod.rs for generated/
    let mut mod_rs = String::new();
    for entry in fs::read_dir(OUTPUT_DIR).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_str().unwrap();
        if name.ends_with(".rs") && name != "mod.rs" {
            let mod_name = name.trim_end_matches(".rs");
            mod_rs.push_str(&format!("pub mod {};\n", mod_name));
        }
    }
    let mod_rs_path = Path::new(OUTPUT_DIR).join("mod.rs");
    fs::write(&mod_rs_path, mod_rs).unwrap();
    format_with_rustfmt(&mod_rs_path);
}

fn generate_columnar_for_struct(s: &ItemStruct, file: &Path) {
    let struct_name = &s.ident;
    let column_struct = format_ident!("{}VecColumns", struct_name);
    let module_path_idents = module_path_from_file(file);
    let struct_path = quote! { crate::#(#module_path_idents::)*#struct_name };
    let out_path = Path::new(OUTPUT_DIR).join(format!(
        "{}_columns.rs",
        struct_name.to_string().to_lowercase()
    ));

    // Collect field names & types
    let fields = match &s.fields {
        syn::Fields::Named(f) => &f.named,
        _ => panic!("Only named structs are supported"),
    };

    let field_defs = fields.iter().map(|f| {
        let name = f.ident.as_ref().unwrap();
        let ty = &f.ty;
        quote! { pub #name: crate::VecColumn<#ty>, }
    });

    let push_body = fields.iter().map(|f| {
        let name = f.ident.as_ref().unwrap();
        quote! { self.#name.push(&row.#name); }
    });

    let merge_body = fields.iter().map(|f| {
        let name = f.ident.as_ref().unwrap();
        quote! { self.#name.merge(other.#name); }
    });

    // Generate columnar struct code
    let expanded = quote! {
        #[derive(Default, Debug)]
        pub struct #column_struct {
            #(#field_defs)*
        }

        impl crate::ColumnBundle<#struct_path> for #column_struct {
            fn push(&mut self, row: &#struct_path) {
                #(#push_body)*
            }

            fn merge(&mut self, other: Self) {
                #(#merge_body)*
            }
        }

        impl crate::Columnar for #struct_path {
            type Columns = #column_struct;
        }
    };

    fs::write(&out_path, expanded.to_string()).unwrap();
    format_with_rustfmt(&out_path);
}

fn module_path_from_file(file: &Path) -> Vec<Ident> {
    fn to_snake_case(name: &str) -> String {
        let mut snake = String::new();
        let mut chars = name.chars().peekable();
        let mut has_prev = false;

        while let Some(ch) = chars.next() {
            if ch.is_uppercase() {
                if has_prev {
                    if let Some(next) = chars.peek() {
                        if next.is_lowercase() || next.is_numeric() {
                            snake.push('_');
                        }
                    }
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

    let relative = file.strip_prefix("src").unwrap_or(file);
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

fn format_with_rustfmt(path: &Path) {
    let Some(path_str) = path.to_str() else {
        return;
    };

    match Command::new("rustfmt")
        .args(["--edition", "2024", path_str])
        .status()
    {
        Ok(status) if status.success() => {}
        Ok(_) => println!("cargo:warning=rustfmt failed on {}", path.display()),
        Err(_) => println!("cargo:warning=rustfmt not found; skipping formatting"),
    }
}
