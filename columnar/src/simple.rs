pub trait SimpleColumnBundle<Row>: Default {
    fn push(&mut self, row: &Row);
    fn merge(&mut self, other: Self);
}

pub trait SimpleColumnar: Sized {
    type Columns: SimpleColumnBundle<Self> + Default;

    fn to_simple_columns(rows: &[Self]) -> Self::Columns {
        let mut cols = Self::Columns::default();
        for r in rows {
            cols.push(r);
        }
        cols
    }
}

/// Simple Vec-backed column, mostly for testing or light use.
#[derive(Debug, Default, Clone)]
pub struct VecColumn<T>(pub Vec<T>);

impl<T: Clone> VecColumn<T> {
    pub fn push(&mut self, v: &T) {
        self.0.push(v.clone());
    }
    pub fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}
