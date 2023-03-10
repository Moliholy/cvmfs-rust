use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDateTime, Utc};
use hex::ToHex;
use rusqlite::Row;

use crate::common::{canonicalize_path, CvmfsError, CvmfsResult, split_md5};
use crate::database_object::DatabaseObject;
use crate::directoryentry::directoryentry::{DirectoryEntry, PathHash};

pub const CATALOG_ROOT_PREFIX: &str = "C";
const LISTING_QUERY: &str = "\
SELECT md5path_1, md5path_2, parent_1, parent_2, hash, flags, size, mode, mtime, name, symlink \
FROM catalog \
WHERE parent_1 = ? AND parent_2 = ? \
ORDER BY name ASC";
const NESTED_COUNT: &str = "SELECT count(*) FROM nested_catalogs;";
const READ_CHUNK: &str = "\
SELECT md5path_1, md5path_2, offset, size, hash \
FROM chunks \
WHERE md5path_1 = ? AND md5path_2 = ? \
ORDER BY offset ASC";
const FIND_MD5_PATH: &str = "SELECT md5path_1, md5path_2, parent_1, parent_2, hash, flags, size, mode, mtime, name, symlink \
FROM catalog \
WHERE md5path_1 = ? AND md5path_2 = ? \
LIMIT 1;";

#[derive(Debug)]
pub struct CatalogReference {
    pub(crate) root_path: String,
    pub(crate) catalog_hash: String,
    pub(crate) catalog_size: u32,
}

/// Wraps the basic functionality of CernVM-FS Catalogs
#[derive(Debug)]
pub struct Catalog {
    database: DatabaseObject,
    schema: f32,
    schema_revision: f32,
    revision: i32,
    previous_revision: String,
    hash: String,
    last_modified: DateTime<Utc>,
    root_prefix: String,
}

unsafe impl Sync for Catalog {}

impl Catalog {
    pub fn new(path: String, hash: String) -> CvmfsResult<Self> {
        let database = DatabaseObject::new(&path)?;
        let properties = database.read_properties_table()?;
        let mut revision = 0;
        let mut previous_revision = String::new();
        let mut schema = 0.0;
        let mut schema_revision = 0.0;
        let mut root_prefix = String::from("/");
        let mut last_modified = Default::default();
        for (key, value) in properties {
            match key.as_str() {
                "revision" => revision = value.parse().unwrap(),
                "schema" => schema = value.parse().unwrap(),
                "schema_revision" => schema_revision = value.parse().unwrap(),
                "last_modified" => last_modified = DateTime::from_utc(
                    NaiveDateTime::from_timestamp_opt(
                        value.parse().unwrap(), 0,
                    ).unwrap(), Utc,
                ),
                "previous_revision" => previous_revision.push_str(&value),
                "root_prefix" => {
                    root_prefix.clear();
                    root_prefix.push_str(&value)
                }
                _ => {}
            }
        }
        if revision == 0 || schema == 0.0 {
            return Err(CvmfsError::CatalogInitialization);
        }
        Ok(Self {
            database,
            schema,
            schema_revision,
            revision,
            hash,
            last_modified,
            root_prefix,
            previous_revision,
        })
    }

    pub fn is_root(&self) -> bool {
        self.root_prefix.eq("/")
    }

    pub fn has_nested(&self) -> CvmfsResult<bool> {
        Ok(self.nested_count()? > 0)
    }

    /// Returns the number of nested catalogs in the catalog
    pub fn nested_count(&self) -> CvmfsResult<u32> {
        let mut result = self.database.create_prepared_statement(NESTED_COUNT)?;
        let mut row = result.query([])?;
        Ok(row.next()?.unwrap().get(0).unwrap())
    }

    /// List CatalogReferences to all contained nested catalogs
    pub fn list_nested(&self) -> CvmfsResult<Vec<CatalogReference>> {
        let new_version = self.schema <= 1.2 && self.schema_revision > 0.0;
        let sql = if new_version {
            "SELECT path, sha1, size FROM nested_catalogs"
        } else {
            "SELECT path, sha1 FROM nested_catalogs"
        };
        let mut result = self.database.create_prepared_statement(sql)?;
        let iterator = result.query_map([], |row| {
            Ok(CatalogReference {
                root_path: row.get(0)?,
                catalog_hash: row.get(1)?,
                catalog_size: if new_version { row.get(2)? } else { 0 },
            })
        })?;
        Ok(iterator.map(|row| row.unwrap()).collect())
    }

    fn path_sanitized(needle_path: &str, catalog_path: &str) -> bool {
        needle_path.len() == catalog_path.len() ||
            (needle_path.len() > catalog_path.len() &&
                needle_path.chars().collect::<Vec<char>>()[catalog_path.len()] == '/')
    }

    /// Find the best matching nested CatalogReference for a given path
    pub fn find_nested_for_path(&self, needle_path: &str) -> CvmfsResult<Option<CatalogReference>> {
        let catalog_refs = self.list_nested()?;
        let mut best_match = None;
        let mut best_match_score = 0;
        let real_needle_path = canonicalize_path(needle_path);
        for nested_catalog in catalog_refs {
            if real_needle_path.starts_with(&nested_catalog.root_path) &&
                nested_catalog.root_path.len() > best_match_score && Self::path_sanitized(needle_path, &nested_catalog.root_path) {
                best_match_score = nested_catalog.root_path.len();
                best_match = Some(nested_catalog);
            }
        }
        Ok(best_match)
    }

    /// Create a directory listing of DirectoryEntry items based on MD5 path
    pub fn list_directory_split_md5(&self, parent_1: u64, parent_2: u64) -> CvmfsResult<Vec<DirectoryEntry>> {
        let mut statement = self.database.create_prepared_statement(LISTING_QUERY)?;
        let mut result = Vec::new();
        let mut rows = statement.query([parent_1, parent_2])?;
        loop {
            match rows.next() {
                Ok(row) => {
                    if let Some(row) = row {
                        result.push(self.make_directory_entry(row)?);
                    } else {
                        break;
                    }
                }
                Err(_) => return Err(CvmfsError::DatabaseError)
            }
        }
        Ok(result)
    }

    pub fn list_directory(&self, path: &str) -> CvmfsResult<Vec<DirectoryEntry>> {
        let mut real_path = canonicalize_path(path);
        if real_path.eq(Path::new("/")) {
            real_path = PathBuf::new();
        }
        let md5_hash = md5::compute(real_path.to_str().unwrap().bytes().collect::<Vec<u8>>());
        let parent_hash = split_md5(&md5_hash.0);
        Ok(self.list_directory_split_md5(parent_hash.hash1, parent_hash.hash2)?)
    }

    fn make_directory_entry(&self, row: &Row) -> CvmfsResult<DirectoryEntry> {
        let mut directory_entry = DirectoryEntry::new(row)?;
        self.read_chunks(&mut directory_entry)?;
        Ok(directory_entry)
    }

    /// Finds and adds the file chunk of a DirectoryEntry
    fn read_chunks(&self, directory_entry: &mut DirectoryEntry) -> CvmfsResult<()> {
        let mut statement = self.database.create_prepared_statement(READ_CHUNK)?;
        let iterator = statement.query([directory_entry.path_hash().hash1, directory_entry.path_hash().hash2])?;
        directory_entry.add_chunks(iterator)?;
        Ok(())
    }

    pub fn find_directory_entry(&self, root_path: &str) -> CvmfsResult<Option<DirectoryEntry>> {
        let real_path = canonicalize_path(root_path);
        let md5_path = md5::compute(real_path.to_str().unwrap().bytes().collect::<Vec<u8>>()).0;
        Ok(self.find_directory_entry_md5(&md5_path)?)
    }

    pub fn find_directory_entry_md5(&self, md5_path: &[u8; 16]) -> CvmfsResult<Option<DirectoryEntry>> {
        let path_hash = split_md5(md5_path);
        Ok(self.find_directory_entry_split_md5(path_hash)?)
    }

    fn find_directory_entry_split_md5(&self, path_hash: PathHash) -> CvmfsResult<Option<DirectoryEntry>> {
        let mut statement = self.database.create_prepared_statement(FIND_MD5_PATH)?;
        let mut rows = statement.query([path_hash.hash1, path_hash.hash2])?;
        if let Some(row) = rows.next()? {
            return Ok(Some(self.make_directory_entry(row)?));
        }
        Ok(None)
    }
}