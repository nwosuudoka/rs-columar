use crate::attr::FieldAttrs;
use syn::{Ident, Type};

/// Metadata for each struct field, extracted during parsing.
#[derive(Clone)]
pub struct FieldSpec {
    pub field_ident: Ident,
    pub field_ty: Type,
    pub column_ident: Ident,
    pub attrs: FieldAttrs,
}
