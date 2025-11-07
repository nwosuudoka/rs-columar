use std::path;

use syn::{Attribute, LitBool, LitInt, LitStr, Result};

#[derive(Debug, Clone, Default)]
pub struct StructAttrs {
    pub chunk_size: Option<usize>,
    pub storage: Option<String>,   // e.g. "vec" | "column" | "stream"
    pub base_path: Option<String>, // where to write files
}

#[derive(Debug, Clone, Default)]
pub struct FieldAttrs {
    pub rename: Option<String>,
    pub skip: bool,
    pub encoder: Option<String>, // e.g. "delta", "fixed", "dict"
    pub path: Option<String>,    // optional per field override path

    pub index: bool,
    pub index_path: Option<String>,
    pub index_type: Option<String>,
}

pub fn parse_struct_attrs(attrs: &[Attribute]) -> Result<StructAttrs> {
    let mut out = StructAttrs::default();
    for a in attrs {
        if !a.path().is_ident("columnar") {
            continue;
        }
        let _ = a.parse_nested_meta(|m| {
            if m.path.is_ident("chunk_size") {
                let lit: LitInt = m.value()?.parse()?;
                let value = lit.base10_parse::<usize>()?;
                out.chunk_size = Some(value);
                Ok(())
            } else if m.path.is_ident("storage") {
                let lit: LitStr = m.value()?.parse()?;
                out.storage = Some(lit.value());
                Ok(())
            } else if m.path.is_ident("base_path") {
                let lit: LitStr = m.value()?.parse()?;
                out.base_path = Some(lit.value());
                Ok(())
            } else {
                Err(m.error("unsupported columnar attribute on struct"))
            }
        });
    }
    Ok(out)
}

pub fn parse_field_attrs(attrs: &[Attribute]) -> Result<FieldAttrs> {
    let mut out = FieldAttrs::default();
    for a in attrs {
        // we check to see if the field is a columnar field
        if !a.path().is_ident("columnar") {
            continue;
        }

        a.parse_nested_meta(|m| {
            // check if we want to rename the field
            if m.path.is_ident("rename") {
                let lit: LitStr = m.value()?.parse()?;
                out.rename = Some(lit.value());
                return Ok(());
            }

            // check if we want to skip the field
            if m.path.is_ident("skip") {
                out.skip = true;
                return Ok(());
            }

            // check the encoder type we are using here
            if m.path.is_ident("encoder") {
                let lit: LitStr = m.value()?.parse()?;
                out.encoder = Some(lit.value());
                return Ok(());
            }

            // check the path we want to encode the file to
            if m.path.is_ident("path") {
                let lit: LitStr = m.value()?.parse()?;
                out.path = Some(lit.value());
                return Ok(());
            }

            if m.path.is_ident("index") {
                if let Ok(lit) = m.value() {
                    let val: LitBool = lit.parse()?;
                    out.index = val.value;
                } else {
                    out.index = true;
                }
                return Ok(());
            }
            if m.path.is_ident("index_type") {
                let lit: LitStr = m.value()?.parse()?;
                out.index_type = Some(lit.value());
                return Ok(());
            }
            if m.path.is_ident("index_path") {
                let lit: LitStr = m.value()?.parse()?;
                out.index_path = Some(lit.value());
                return Ok(());
            }

            Err(m.error("unsupported columnar attribute on field"))
        })?;
    }
    Ok(out)
}
