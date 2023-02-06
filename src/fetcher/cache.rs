use std::fs::{create_dir_all, remove_dir_all};
use std::path::{Path, PathBuf};

use faccess::PathExt;

use crate::common::{CvmfsError, CvmfsResult};

#[derive(Debug)]
pub struct Cache {
    pub cache_directory: String,
}

impl Cache {
    pub fn new(cache_directory: String) -> CvmfsResult<Self> {
        let path = Path::new(&cache_directory);
        Ok(Self {
            cache_directory: path.to_str().unwrap().into()
        })
    }

    pub fn initialize(&self) -> CvmfsResult<()> {
        let base_path = self.create_directory("data")?;
        for i in 0x00..=0xff {
            let new_folder = format!("{:02x}", i);
            let new_file = Path::join::<&Path>(base_path.as_ref(), new_folder.as_ref());
            create_dir_all(new_file)?;
        }
        Ok(())
    }

    fn create_directory(&self, path: &str) -> CvmfsResult<String> {
        let cache_full_path = Path::new(&self.cache_directory).join(path);
        create_dir_all(cache_full_path.clone())?;
        Ok(cache_full_path.into_os_string().into_string().unwrap())
    }

    pub fn add(&self, file_name: &str) -> PathBuf {
        Path::join(self.cache_directory.as_ref(), file_name)
    }

    pub fn get(&self, file_name: &str) -> Option<PathBuf> {
        let path = self.add(file_name);
        if path.exists() || path.is_file() {
            return Some(path);
        }
        None
    }

    pub fn evict(&self) -> CvmfsResult<()> {
        let data_path = Path::new(&self.cache_directory).join("data");
        if data_path.exists() && data_path.is_dir() {
            remove_dir_all(data_path)?;
            self.initialize()?;
        }
        Ok(())
    }
}