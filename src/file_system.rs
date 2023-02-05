use std::collections::HashMap;
use std::fs::File;

use fuse_mt::{FilesystemMT, RequestInfo, ResultEmpty};

use crate::common::CvmfsResult;
use crate::repository::Repository;
use crate::revision::Revision;

#[derive(Debug)]
pub struct CernvmFileSystem<'repo> {
    repository: Repository,
    revision: Option<Revision<'repo>>,
}

impl<'repo> FilesystemMT for CernvmFileSystem<'repo> {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Ok(())
    }

    fn destroy(&self) {
        // Nothing to do
    }
}

impl<'repo> CernvmFileSystem<'repo> {
    pub fn new(repository: Repository) -> CvmfsResult<Self> {
        Ok(Self {
            repository,
            revision: None,
        })
    }

    pub fn set_default_revision(&'repo mut self) -> CvmfsResult<()> {
        self.revision = Some(self.repository.get_current_revision()?);
        Ok(())
    }

    pub fn set_revision(&'repo mut self, revision: u32) -> CvmfsResult<()> {
        self.revision = Some(self.repository.get_revision(revision)?);
        Ok(())
    }
}