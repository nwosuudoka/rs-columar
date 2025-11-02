#[derive(Default, Debug)]
pub struct PersonVecColumns {
    pub id: crate::VecColumn<u64>,
    pub name: crate::VecColumn<String>,
}
impl crate::ColumnBundle<crate::models::person::Person> for PersonVecColumns {
    fn push(&mut self, row: &crate::models::person::Person) {
        self.id.push(&row.id);
        self.name.push(&row.name);
    }
    fn merge(&mut self, other: Self) {
        self.id.merge(other.id);
        self.name.merge(other.name);
    }
}
impl crate::Columnar for crate::models::person::Person {
    type Columns = PersonVecColumns;
}
