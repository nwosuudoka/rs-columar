pub trait IntoColumns {
    fn to_simple_columns(&self) -> <Self as SimpleColumnar>::Columns
    where
        Self: SimpleColumnar,
    {
        let mut cols = Self::Columns::default();
        cols.push(self);
        cols
    }

    fn to_streaming_columns(&self) -> io::Result<<Self as StreamingColumnar>::Columns>
    where
        Self: StreamingColumnar,
    {
        let mut cols = Self::Columns::default();
        cols.push(self)?;
        Ok(cols)
    }
}
