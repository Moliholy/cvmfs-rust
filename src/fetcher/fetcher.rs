use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use compress::zlib;
use futures::executor::block_on;

use crate::common::{CvmfsError, CvmfsResult};
use crate::fetcher::cache::Cache;

pub struct Fetcher {
    cache: Cache,
    source: String,
}

impl Fetcher {
    pub fn new(source: &str, cache_directory: &str) -> CvmfsResult<Self> {
        let path = Path::new(source);
        let source = if path.exists() && path.is_dir() {
            format!("{}{}", "file://", source)
        } else {
            source.into()
        };
        let cache = Cache::new(cache_directory.into())?;
        cache.initialize()?;
        Ok(Self {
            cache,
            source,
        })
    }

    ///Method to retrieve a file from the cache if exists, or from
    ///the repository if it doesn't. In case it has to be retrieved from
    ///the repository it won't be decompressed
    pub fn retrieve_raw_file(&self, file_name: &str) -> CvmfsResult<String> {
        let cache_file = self.cache.add(file_name);
        let file_url = self.make_file_url(file_name);
        block_on(Self::download_content_and_store(cache_file.to_str().unwrap(), file_url.to_str().unwrap()))?;
        Ok(self.cache.get(file_name).unwrap().to_str().unwrap().into())
    }

    pub fn retrieve_file(&self, file_name: &str) -> CvmfsResult<String> {
        if let Some(cached_file) = self.cache.get(file_name) {
            return Ok(cached_file.to_str().unwrap().into());
        }
        Ok(self.retrieve_file_from_source(file_name)?)
    }

    fn make_file_url(&self, file_name: &str) -> PathBuf {
        Path::join(self.source.as_ref(), file_name)
    }

    fn retrieve_file_from_source(&self, file_name: &str) -> CvmfsResult<String> {
        let file_url = self.make_file_url(file_name);
        let cached_file = self.cache.add(file_name);
        block_on(Self::download_content_and_decompress(cached_file.to_str().unwrap(), file_url.to_str().unwrap()))?;
        match self.cache.get(file_name) {
            None => Err(CvmfsError::FileNotFound),
            Some(file) => Ok(file.to_str().unwrap().into())
        }
    }

    async fn download_content_and_decompress(cached_file: &str, file_url: &str) -> CvmfsResult<()> {
        let file_bytes = reqwest::get(file_url).await?.bytes().await?;
        Self::decompress(file_bytes.as_ref(), cached_file)?;
        Ok(())
    }

    async fn download_content_and_store(cached_file: &str, file_url: &str) -> CvmfsResult<()> {
        let content = reqwest::get(file_url).await?.bytes().await?.to_vec();
        fs::write(cached_file, content)?;
        Ok(())
    }

    fn decompress(compressed_bytes: &[u8], cached_file: &str) -> CvmfsResult<()> {
        let mut decompressed = Vec::new();
        zlib::Decoder::new(compressed_bytes).read_to_end(&mut decompressed)?;
        fs::write(cached_file, decompressed)?;
        Ok(())
    }
}