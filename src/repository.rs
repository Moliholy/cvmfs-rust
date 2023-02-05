use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::Path;

use chrono::{DateTime, Utc};

use crate::catalog::catalog::{Catalog, CATALOG_ROOT_PREFIX};
use crate::common::{CvmfsError, CvmfsResult, LAST_REPLICATION_NAME, MANIFEST_NAME, REPLICATING_NAME};
use crate::directoryentry::directoryentry::DirectoryEntry;
use crate::fetcher::fetcher::Fetcher;
use crate::history::History;
use crate::manifest::Manifest;
use crate::revision_tag::RevisionTag;
use crate::rootfile::RootFile;

/// Wrapper around a CVMFS repository representation
#[derive(Debug)]
pub struct Repository {
    pub opened_catalogs: HashMap<String, Catalog>,
    pub manifest: Manifest,
    pub fqrn: String,
    pub repo_type: String,
    pub replicating_since: Option<DateTime<Utc>>,
    pub last_replication: Option<DateTime<Utc>>,
    pub replicating: bool,
    fetcher: Fetcher,
    tag: Option<RevisionTag>,
}

impl Repository {
    pub fn new(fetcher: Fetcher) -> CvmfsResult<Self> {
        let manifest = Self::read_manifest(&fetcher)?;
        let last_replication = Self::try_to_get_last_replication_timestamp(&fetcher).unwrap_or(None);
        let replicating_since = Self::try_to_get_replication_state(&fetcher).unwrap_or(None);
        let mut obj = Self {
            opened_catalogs: HashMap::new(),
            fqrn: manifest.repository_name.clone(),
            manifest,
            repo_type: "stratum1".to_string(),
            replicating_since,
            last_replication,
            replicating: replicating_since.is_some(),
            fetcher,
            tag: None,
        };
        obj.tag = Some(obj.get_last_tag()?.clone());
        Ok(obj)
    }

    /// Retrieves an object from the content addressable storage
    pub fn retrieve_object(&self, object_hash: &str) -> CvmfsResult<String> {
        self.retrieve_object_with_suffix(object_hash, "")
    }

    pub fn retrieve_object_with_suffix(&self, object_hash: &str, hash_suffix: &str) -> CvmfsResult<String> {
        let (first, second) = object_hash.split_at(2);
        let path = Path::new("data").join(first).join(second.to_owned() + hash_suffix);
        Ok(self.fetcher.retrieve_file(path.to_str().unwrap())?)
    }

    /// Download and open a catalog from the repository
    pub fn retrieve_catalog(&mut self, catalog_hash: &str) -> CvmfsResult<&Catalog> {
        if self.opened_catalogs.contains_key(catalog_hash) {
            return Ok(&self.opened_catalogs[catalog_hash]);
        }
        self.retrieve_and_open_catalog(catalog_hash)
    }

    pub fn retrieve_and_open_catalog(&mut self, catalog_hash: &str) -> CvmfsResult<&Catalog> {
        let catalog_file = self.retrieve_object_with_suffix(catalog_hash, CATALOG_ROOT_PREFIX)?;
        let catalog = Catalog::new(catalog_file, catalog_hash.into())?;
        self.opened_catalogs.insert(catalog_hash.into(), catalog);
        Ok(self.opened_catalogs.get(catalog_hash.into()).unwrap())
    }

    pub fn has_history(&self) -> bool {
        self.manifest.has_history()
    }

    pub fn retrieve_history(&self) -> CvmfsResult<History> {
        if !self.has_history() {
            return Err(CvmfsError::HistoryNotFound);
        }
        let history_db = self.retrieve_object_with_suffix(&self.manifest.history_database.as_ref().unwrap(), "H")?;
        Ok(History::new(&history_db)?)
    }

    pub fn get_tag(&mut self, number: u32) -> CvmfsResult<RevisionTag> {
        let history = self.retrieve_history()?;
        let tag = history.get_tag_by_revision(number)?;
        match tag {
            None => Err(CvmfsError::RevisionNotFound),
            Some(tag) => Ok(tag)
        }
    }

    pub fn current_tag(&self) -> &RevisionTag {
        self.tag.as_ref().unwrap()
    }

    pub fn set_current_tag(&mut self, number: u32) -> CvmfsResult<()> {
        self.tag = Some(self.get_tag(number)?);
        Ok(())
    }

    pub fn get_last_tag(&mut self) -> CvmfsResult<RevisionTag> {
        self.get_tag(self.manifest.revision)
    }

    fn read_manifest(fetcher: &Fetcher) -> CvmfsResult<Manifest> {
        let manifest_file = fetcher.retrieve_raw_file(MANIFEST_NAME)?;
        let file = File::open(&manifest_file)?;
        let root_file = RootFile::new(&file)?;
        Ok(Manifest::new(root_file))
    }

    fn get_replication_date(fetcher: &Fetcher, file_name: &str) -> CvmfsResult<Option<DateTime<Utc>>> {
        let file = fetcher.retrieve_raw_file(file_name)?;
        let date_string = fs::read_to_string(&file)?;
        let date = DateTime::parse_from_str(&date_string, "%a %e %h %H:%M:%S %Z %Y");
        match date {
            Ok(date) => Ok(Some(DateTime::from(date))),
            Err(_) => Ok(None)
        }
    }

    fn try_to_get_last_replication_timestamp(fetcher: &Fetcher) -> CvmfsResult<Option<DateTime<Utc>>> {
        Self::get_replication_date(fetcher, LAST_REPLICATION_NAME)
    }

    fn try_to_get_replication_state(fetcher: &Fetcher) -> CvmfsResult<Option<DateTime<Utc>>> {
        Self::get_replication_date(fetcher, REPLICATING_NAME)
    }

    pub fn get_revision_number(&self) -> i32 {
        self.current_tag().revision
    }

    pub fn get_root_hash(&self) -> &str {
        &self.current_tag().hash
    }

    pub fn get_name(&self) -> &str {
        &self.current_tag().name
    }

    pub fn get_timestamp(&self) -> u64 {
        self.current_tag().timestamp
    }

    pub fn retrieve_current_root_catalog(&mut self) -> CvmfsResult<&Catalog> {
        let root_hash = self.current_tag().hash.to_string();
        Ok(self.retrieve_catalog(&root_hash)?)
    }

    /// Recursively walk down the Catalogs and find the best fit for a path
    pub fn retrieve_catalog_for_path(&mut self, needle_path: &str) -> CvmfsResult<&Catalog> {
        let mut hash = String::from(self.get_root_hash());
        loop {
            match self.retrieve_catalog(&hash)?.find_nested_for_path(needle_path) {
                Ok(None) => return Ok(self.retrieve_catalog(&hash).unwrap()),
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
                return Ok(Some(self.retrieve_object(&directory_entry.content_hash_string())?));
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