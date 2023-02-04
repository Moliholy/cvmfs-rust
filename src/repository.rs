use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::Path;

use chrono::{DateTime, Utc};

use crate::catalog::catalog::{Catalog, CATALOG_ROOT_PREFIX};
use crate::common::{CvmfsResult, LAST_REPLICATION_NAME, MANIFEST_NAME, REPLICATING_NAME};
use crate::fetcher::fetcher::Fetcher;
use crate::manifest::Manifest;
use crate::rootfile::RootFile;

/// Wrapper around a CVMFS repository representation
pub struct Repository {
    opened_catalogs: HashMap<String, Catalog>,
    manifest: Manifest,
    fqrn: String,
    repo_type: String,
    replicating_since: Option<DateTime<Utc>>,
    last_replication: Option<DateTime<Utc>>,
    replicating: bool,
    fetcher: Fetcher,
}

impl Repository {
    pub fn new(fetcher: Fetcher) -> CvmfsResult<Self> {
        let manifest = Self::read_manifest(&fetcher)?;
        let last_replication = Self::try_to_get_last_replication_timestamp(&fetcher).unwrap_or(None);
        let replicating_since = Self::try_to_get_replication_state(&fetcher).unwrap_or(None);
        Ok(Self {
            opened_catalogs: HashMap::new(),
            fqrn: manifest.repository_name.clone(),
            manifest,
            repo_type: "stratum1".to_string(),
            replicating_since,
            last_replication,
            replicating: replicating_since.is_some(),
            fetcher,
        })
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
    
    fn read_manifest(fetcher: &Fetcher) -> CvmfsResult<Manifest> {
        let manifest_file = fetcher.retrieve_raw_file(MANIFEST_NAME)?;
        let file = File::open(&manifest_file)?;
        Ok(Manifest::new(RootFile::new(&file)?))
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
}