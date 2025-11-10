use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn tempdir() -> io::Result<TempDir> {
    TempDir::new()
}

pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new() -> io::Result<Self> {
        let temp_dir = env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dirname = format!("test_{}_{}", process::id(), timestamp);
        let path = temp_dir.join(dirname);
        fs::create_dir(&path)?;
        Ok(TempDir { path })
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        #[allow(unused_must_use)]
        let _ = fs::remove_dir_all(&self.path);
    }
}
