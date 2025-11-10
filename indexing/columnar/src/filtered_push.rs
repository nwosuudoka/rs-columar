use std::{collections::HashSet, io};

#[derive(Debug, Clone)]
pub struct PushConfig {
    allowed_fields: HashSet<String>,
}

impl PushConfig {
    /// Creates a new `PushConfig` with the given set of allowed fields.
    ///
    /// `fields` is an iterator over values that can be converted to `&str`.
    /// The resulting `PushConfig` will allow pushing values to fields that are in the set of
    /// allowed fields, and will forbid pushing to any other fields.
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let allowed_fields = fields.into_iter().map(|s| s.as_ref().to_string()).collect();
        Self { allowed_fields }
    }

    pub fn is_allowed(&self, field: &str) -> bool {
        self.allowed_fields.contains(field)
    }
}

pub trait FilteredPush<Row> {
    fn push_with_config(&mut self, row: &Row, cfg: &crate::PushConfig) -> io::Result<()>;
}
