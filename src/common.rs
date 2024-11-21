use std::fmt::Debug;
use std::path::PathBuf;

use crate::directory_entry::PathHash;

pub const REPO_CONFIG_PATH: &str = "/etc/cvmfs/repositories.d";
pub const SERVER_CONFIG_NAME: &str = "server.conf";
pub const REST_CONNECTOR: &str = "control";
pub const WHITELIST_NAME: &str = ".cvmfswhitelist";
pub const MANIFEST_NAME: &str = ".cvmfspublished";
pub const LAST_REPLICATION_NAME: &str = ".cvmfs_last_snapshot";
pub const REPLICATING_NAME: &str = ".cvmfs_is_snapshotting";

pub type CvmfsResult<R> = Result<R, CvmfsError>;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum CvmfsError {
    #[error("Invalid Certificate")]
    Certificate,
    #[error("IO error")]
    IO(String),
    #[error("Incomplete root file signature")]
    IncompleteRootFileSignature,
    #[error("Invalid root file signature")]
    InvalidRootFileSignature,
    #[error("Cache directory not found")]
    CacheDirectoryNotFound,
    #[error("DatabaseError")]
    DatabaseError(String),
    #[error("Catalog initialization")]
    CatalogInitialization,
    #[error("File not found")]
    FileNotFound,
    #[error("History not found")]
    HistoryNotFound,
    #[error("Revision not found")]
    RevisionNotFound,
    #[error("Invalid timestamp")]
    InvalidTimestamp,
    #[error("Parse error")]
    ParseError,
    #[error("Synchronization error")]
    Sync,
    #[error("Catalog not found")]
    CatalogNotFound,
    #[error("Tag not found")]
    TagNotFound,
    #[error("Generic error")]
    Generic(String),
}

impl From<String> for CvmfsError {
    fn from(value: String) -> Self {
        CvmfsError::Generic(value)
    }
}

impl From<&str> for CvmfsError {
    fn from(value: &str) -> Self {
        CvmfsError::Generic(value.to_string())
    }
}

impl From<CvmfsError> for i32 {
    fn from(_: CvmfsError) -> Self {
        libc::ENOSYS
    }
}

impl From<reqwest::Error> for CvmfsError {
    fn from(e: reqwest::Error) -> Self {
        CvmfsError::IO(format!("{:?}", e))
    }
}

impl From<std::io::Error> for CvmfsError {
    fn from(e: std::io::Error) -> Self {
        CvmfsError::IO(format!("{:?}", e))
    }
}

impl From<rusqlite::Error> for CvmfsError {
    fn from(e: rusqlite::Error) -> Self {
        CvmfsError::DatabaseError(format!("{:?}", e))
    }
}

pub fn canonicalize_path(path: &str) -> PathBuf {
    PathBuf::from(path)
        .canonicalize()
        .unwrap_or(PathBuf::from(path))
}

pub fn split_md5(md5_digest: &[u8; 16]) -> PathHash {
    let mut hi = 0;
    let mut lo = 0;
    for i in 0..8 {
        lo |= (md5_digest[i] as i64) << (i * 8);
    }
    for i in 8..16 {
        hi |= (md5_digest[i] as i64) << ((i - 8) * 8)
    }
    PathHash {
        hash1: lo,
        hash2: hi,
    }
}
