use columnar_derive::{ColumnarAttrs, StreamingColumnar};

#[derive(ColumnarAttrs)]
#[columnar(base_path = "data/out")]
pub struct Person {
    #[columnar(encoder = "bitpack")]
    pub id: u64,
}
