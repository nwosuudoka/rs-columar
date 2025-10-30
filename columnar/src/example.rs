use crate::SimpleColumnar;

#[derive(SimpleColumnar, Default, Debug, Clone)]
pub struct SimpleExample {
    pub id: u64,
    pub score: f32,
}
