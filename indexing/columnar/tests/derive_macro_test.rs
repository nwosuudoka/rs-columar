use columnar::Columnar; // trait re-exported from columnar crate

#[derive(Columnar, Default, Debug, Clone)]
pub struct TestStruct {
    pub a: i32,
    pub b: f32,
}

#[test]
fn test_columnar_macro_and_runtime() {
    let rows = vec![
        TestStruct { a: 1, b: 1.0 },
        TestStruct { a: 2, b: 2.0 },
        TestStruct { a: 3, b: 3.0 },
    ];

    // This calls the generated impl from #[derive(Columnar)]
    let cols = TestStruct::to_columns(&rows);

    // Check lengths
    assert_eq!(cols.a.len(), 3);
    assert_eq!(cols.b.len(), 3);

    // Check values
    let collected_a: Vec<_> = cols
        .a
        .chunks
        .iter()
        .flat_map(|chunk| chunk.iter())
        .cloned()
        .collect();
    let collected_b: Vec<_> = cols
        .b
        .chunks
        .iter()
        .flat_map(|chunk| chunk.iter())
        .cloned()
        .collect();
    assert_eq!(collected_a, vec![1, 2, 3]);
    assert_eq!(collected_b, vec![1.0, 2.0, 3.0]);
}
