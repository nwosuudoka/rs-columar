use std::path::PathBuf;

use tempfile::TempDir;
use toolkit::temp;

use crate::{StreamingColumnBundle, models::person::Person};

#[derive(Debug)]
pub struct PersonStreamColumn {
    pub id: crate::StreamColumn<u64>,
}
impl PersonStreamColumn {
    fn with_pool(pool: crate::SmartBufferPool, temp_dir: std::path::PathBuf) -> Self {
        Self {
            id: crate::StreamColumn::new(
                "data/out/Person/id.bin",
                pool.clone(),
                Box::new(crate::encoding::BitpackStreamWriter::<u64>::new(
                    pool.clone(),
                )),
                None,
                temp_dir.clone(),
            )
            .unwrap(),
        }
    }
}
impl crate::StreamingColumnBundle<crate::models::person::Person> for PersonStreamColumn {
    fn push(&mut self, row: &crate::models::person::Person) -> std::io::Result<()> {
        self.id.push(&row.id.clone())?;
        Ok(())
    }
}
impl crate::StreamingColumnar for crate::models::person::Person {
    type Columns = PersonStreamColumn;
}
impl crate::FilteredPush<crate::models::person::Person> for PersonStreamColumn {
    fn push_with_config(
        &mut self,
        row: &crate::models::person::Person,
        cfg: &crate::PushConfig,
    ) -> std::io::Result<()> {
        if cfg.is_allowed("id") {
            self.id.push(&row.id.clone())?;
        }
        Ok(())
    }
}

#[test]
fn test_columns() {
    let pool = crate::SmartBufferPool::new(4 * 1024);
    let mut temp = TempDir::new().unwrap();
    let path = temp.path().clone();
    let mut person_columns = PersonStreamColumn::with_pool(pool, PathBuf::from(path));

    let person = crate::models::person::Person { id: 1 };
    person_columns.push(&person).unwrap();
}
