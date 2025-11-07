pub trait Tokenizer {
    fn tokenize(&self, text: &str) -> Vec<u64>;
}
