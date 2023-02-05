use std::collections::HashMap;
use std::fs::File;

use fuse_mt::{FilesystemMT, RequestInfo, ResultEmpty};

use crate::common::CvmfsResult;
use crate::repository::Repository;

#[derive(Debug)]
pub struct CernvmFileSystem {
    repository: Repository,
}

impl FilesystemMT for CernvmFileSystem {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Ok(())
    }

    fn destroy(&self) {
        // Nothing to do
    }
}

impl CernvmFileSystem {
    pub fn new(repository: Repository) -> CvmfsResult<Self> {
        Ok(Self {
            repository,
        })
    }
}