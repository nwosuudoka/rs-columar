#[derive(Default)]
pub struct SimpleExampleVecColumns {
    pub id: Vec<u64>,
    pub score: Vec<f32>,
}
impl crate::ColumnStorageBundle<SimpleExample> for SimpleExampleVecColumns {
    fn push(&mut self, row: &SimpleExample) {
        self.id.push(row.id.clone());
        self.score.push(row.score.clone());
    }
    fn merge(&mut self, other: Self) {
        self.id.extend(other.id);
        self.score.extend(other.score);
    }
    fn set_chunk_size(&mut self, _: usize) {}
}
impl crate::Columnar for SimpleExample {
    type Columns = SimpleExampleVecColumns;
}
