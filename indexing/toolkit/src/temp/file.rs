use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TempFile {
    path: PathBuf,
}

impl Default for TempFile {
    fn default() -> Self {
        let temp_dir = env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let filename = format!("test_{}_{}.bin", process::id(), timestamp);
        let path = temp_dir.join(filename);

        fs::File::create(&path).expect("failed to create temp file");
        TempFile { path }
    }
}

impl TempFile {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
