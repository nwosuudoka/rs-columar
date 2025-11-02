use syn::{Attribute, LitInt, LitStr, Result};

#[derive(Debug, Clone, Default)]
pub struct StructAttrs {
    pub chunk_size: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct FieldAttrs {
    pub rename: Option<String>,
    pub skip: bool,
}

pub fn parse_struct_attrs(attrs: &[Attribute]) -> Result<StructAttrs> {
    let mut sa = StructAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("columnar") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("chunk_size") {
                let lit: LitInt = meta.value()?.parse()?;
                let value = lit.base10_parse::<usize>()?;
                if sa.chunk_size.replace(value).is_some() {
                    return Err(meta.error("duplicate columnar(chunk_size) attribute"));
                }
                Ok(())
            } else {
                Err(meta.error("unsupported columnar attribute on struct"))
            }
        })?;
    }
    Ok(sa)
}

pub fn parse_field_attrs(attrs: &[Attribute]) -> Result<FieldAttrs> {
    let mut fa = FieldAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("columnar") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                let lit: LitStr = meta.value()?.parse()?;
                if fa.rename.replace(lit.value()).is_some() {
                    return Err(meta.error("duplicate columnar(rename) attribute"));
                }
                Ok(())
            } else if meta.path.is_ident("skip") {
                if !meta.input.is_empty() {
                    return Err(meta.error("columnar(skip) does not take a value"));
                }
                fa.skip = true;
                Ok(())
            } else {
                Err(meta.error("unsupported columnar attribute on field"))
            }
        })?;
    }
    Ok(fa)
}
