use columnar_derive::SimpleColumnar;

#[derive(SimpleColumnar)]
pub struct Person {
    pub id: u64,
    pub name: String,
}
