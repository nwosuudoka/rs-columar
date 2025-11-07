use std::error::Error;
use std::fmt;

pub struct CapacityError;

impl std::fmt::Debug for CapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "capacity exceeded")
    }
}

impl fmt::Display for CapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "capacity exceeded")
    }
}

impl Error for CapacityError {}
