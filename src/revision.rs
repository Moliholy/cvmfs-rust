use rusqlite::Row;

use crate::catalog::catalog::Catalog;
use crate::common::{CvmfsError, CvmfsResult};
use crate::directoryentry::directoryentry::DirectoryEntry;
use crate::repository::Repository;

pub const SQL_QUERY_ALL: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
ORDER BY timestamp DESC";

pub const SQL_QUERY_NAME: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
WHERE name = ? \
LIMIT 1";

pub const SQL_QUERY_REVISION: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
WHERE revision = ? \
LIMIT 1";

pub const SQL_QUERY_DATE: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
WHERE timestamp > ? \
ORDER BY timestamp ASC \
LIMIT 1";

#[derive(Debug, Clone)]
pub struct RevisionTag {
    pub(crate) name: String,
    pub(crate) hash: String,
    pub(crate) revision: i32,
    pub(crate) timestamp: u64,
    pub(crate) channel: i32,
    pub(crate) description: String,
}

impl RevisionTag {
    pub fn new(row: &Row) -> CvmfsResult<Self> {
        Ok(Self {
            name: row.get(0)?,
            hash: row.get(1)?,
            revision: row.get(2)?,
            timestamp: row.get(3)?,
            channel: row.get(4)?,
            description: row.get(5)?,
        })
    }
}

/// Wrapper around a CVMFS Repository revision.
/// A Revision is a concrete instantiation in time of the Repository. It
/// represents the concrete status of the repository in a certain period of
/// time. Revision data is contained in the so-called Tags, which are stored in
/// the History database.
#[derive(Debug)]
pub struct Revision<'repo> {
    repository: &'repo mut Repository,
    tag: RevisionTag,
}

impl<'repo> Revision<'repo> {
    pub fn new(repository: &'repo mut Repository, tag: RevisionTag) -> Self {
        Self {
            repository,
            tag,
        }
    }

    pub fn get_revision_number(&self) -> i32 {
        self.tag.revision
    }

    pub fn get_root_hash(&self) -> &str {
        &self.tag.hash
    }

    pub fn get_name(&self) -> &str {
        &self.tag.name
    }

    pub fn get_timestamp(&self) -> u64 {
        self.tag.timestamp
    }

    pub fn retrieve_root_catalog(&mut self) -> CvmfsResult<&Catalog> {
        let root_hash = self.get_root_hash().to_string();
        Ok(self.retrieve_catalog(&root_hash)?)
    }

    /// Retrieve and open a catalog that belongs to this revision
    pub fn retrieve_catalog(&mut self, catalog_hash: &str) -> CvmfsResult<&Catalog> {
        Ok(self.repository.retrieve_catalog(catalog_hash)?)
    }

    /// Recursively walk down the Catalogs and find the best fit for a path
    pub fn retrieve_catalog_for_path(&mut self, needle_path: &str) -> CvmfsResult<&Catalog> {
        let mut hash = String::from(self.get_root_hash());
        loop {
            match self.retrieve_catalog(&hash)?.find_nested_for_path(needle_path) {
                Ok(None) => return Ok(self.repository.retrieve_catalog(&hash).unwrap()),
                Ok(Some(nested_reference)) => hash = nested_reference.catalog_hash.clone(),
                Err(error) => return Err(error)
            };
        }
    }

    pub fn lookup(&mut self, path: &str) -> CvmfsResult<Option<DirectoryEntry>> {
        let mut path = String::from(path);
        if path.eq("/") {
            path = String::new();
        }
        let best_fit = self.retrieve_catalog_for_path(&path)?;
        Ok(best_fit.find_directory_entry(&path)?)
    }

    pub fn get_file(&mut self, path: &str) -> CvmfsResult<Option<String>> {
        let result = self.lookup(path)?;
        if let Some(directory_entry) = result {
            if directory_entry.is_file() {
                return Ok(Some(self.repository.retrieve_object(&directory_entry.content_hash_string())?));
            }
        }
        Err(CvmfsError::FileNotFound)
    }

    /// List all the entries in a directory
    pub fn list_directory(&mut self, path: &str) -> CvmfsResult<Vec<DirectoryEntry>> {
        let directory_entry = self.lookup(path)?;
        if let Some(dirent) = directory_entry {
            if dirent.is_directory() {
                let best_fit = self.retrieve_catalog_for_path(path)?;
                return Ok(best_fit.list_directory(path)?);
            }
        }
        Err(CvmfsError::FileNotFound)
    }
}