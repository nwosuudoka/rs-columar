#[derive(Debug)]
pub struct PersonStreamColumn {
    pub id: crate::StreamColumn<u64>,
}
impl PersonStreamColumn {
    fn with_pool(pool: crate::SmartBufferPool) -> Self {
        Self {
            id: crate::StreamColumn::new(
                "data/out/Person/id.bin",
                Box::new(crate::encoding::BitpackStreamWriter::<u64>::new(
                    pool.clone(),
                )),
                pool.clone(),
            )
            .unwrap(),
        }
    }
}
impl Default for PersonStreamColumn {
    fn default() -> Self {
        let pool = crate::SmartBufferPool::new(64 * 1024);
        Self::with_pool(pool)
    }
}
impl crate::StreamingColumnBundle<crate::models::person::Person> for PersonStreamColumn {
    fn push(&mut self, row: &crate::models::person::Person) -> std::io::Result<()> {
        self.id.push(&row.id.clone())?;
        Ok(())
    }
    fn merge(&mut self, other: Self) {
        self.id.merge(other.id);
    }
}
impl crate::StreamingColumnar for crate::models::person::Person {
    type Columns = PersonStreamColumn;
}
impl crate::FilteredPush<crate::models::person::Person> for PersonStreamColumn {
    fn push_with_config(&mut self, row: &crate::models::person::Person, cfg: &crate::PushConfig) {
        if cfg.is_allowed("id") {
            self.id.push(&row.id.clone());
        }
    }
}
