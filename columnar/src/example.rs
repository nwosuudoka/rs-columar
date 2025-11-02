use crate::SimpleColumnar;
#[allow(unused_imports)]
use crate::{ColumnBundle, Columnar};

#[derive(SimpleColumnar, Default, Debug, Clone)]
pub struct SimpleExample {
    pub id: u64,
    pub score: f32,
}

#[derive(SimpleColumnar, Debug)]
pub struct Person {
    pub id: u64,
    #[columnar(rename = "user_name")]
    pub name: String,
    #[columnar(skip)]
    pub temp_value: f32,
}

#[test]
fn example_usage() {
    let rows = vec![
        Person {
            id: 1,
            name: "Alice".into(),
            temp_value: 0.1,
        },
        Person {
            id: 2,
            name: "Bob".into(),
            temp_value: 0.2,
        },
    ];

    let mut cols = Person::to_columns(&rows);
    println!("Columns: {:?}", cols);

    let other = Person::to_columns(&[Person {
        id: 3,
        name: "Carol".into(),
        temp_value: 0.3,
    }]);
    cols.merge(other);
    println!("After merge: {:?}", cols);
}
