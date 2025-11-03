#[derive(Debug, Default)]
pub struct PersonVecColumns {
    pub id: crate::VecColumn<u64>,
    pub name: crate::VecColumn<String>,
}
impl crate::SimpleColumnBundle<crate::models::person::Person> for PersonVecColumns {
    fn push(&mut self, row: &crate::models::person::Person) {
        self.id.push(&row.id.clone());
        self.name.push(&row.name.clone());
    }
    fn merge(&mut self, other: Self) {
        self.id.merge(other.id);
        self.name.merge(other.name);
    }
}
impl crate::SimpleColumnar for crate::models::person::Person {
    type Columns = PersonVecColumns;
}
