use std::fmt::Debug;
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};

use crate::directory_entry::{Chunk, PathHash};
use crate::fetcher::Fetcher;

pub const REPO_CONFIG_PATH: &str = "/etc/cvmfs/repositories.d";
pub const SERVER_CONFIG_NAME: &str = "server.conf";
pub const REST_CONNECTOR: &str = "control";
pub const WHITELIST_NAME: &str = ".cvmfswhitelist";
pub const MANIFEST_NAME: &str = ".cvmfspublished";
pub const LAST_REPLICATION_NAME: &str = ".cvmfs_last_snapshot";
pub const REPLICATING_NAME: &str = ".cvmfs_is_snapshotting";

pub type CvmfsResult<R> = Result<R, CvmfsError>;
pub trait FileLike: Debug + Read + Seek + AsRawFd + Send + Sync {}

impl FileLike for File {}

#[derive(Debug)]
pub struct ChunkedFile {
    size: u64,
    chunks: Vec<(String, Chunk)>,
    position: u64,
    fetcher: Fetcher,
}

impl ChunkedFile {
    pub(crate) fn new(chunks: Vec<(String, Chunk)>, size: u64, fetcher: Fetcher) -> Self {
        Self {
            chunks,
            position: 0,
            size,
            fetcher,
        }
    }
}

impl Read for ChunkedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut currently_read = 0;
        let mut index = match self.chunks.iter().position(|(_, chunk)| {
            chunk.offset >= self.position && chunk.offset + chunk.size < self.position
        }) {
            None => return Err(ErrorKind::UnexpectedEof.into()),
            Some(i) => i,
        };
        while currently_read < buf.len() && index < self.chunks.len() {
            let (path, chunk) = &self.chunks[index];
            let chunk_position = self.position - chunk.offset;
            let local_path = self
                .fetcher
                .retrieve_file(path.as_str())
                .map_err(|_| ErrorKind::Unsupported)?;
            let mut file = File::open(local_path).map_err(|_| ErrorKind::NotFound)?;
            file.seek(SeekFrom::Start(chunk_position))
                .map_err(|_| ErrorKind::NotSeekable)?;
            let mut chunk_bytes_read = 0;
            while chunk_bytes_read < buf.len() {
                let bytes_read = file.read(buf)?;
                if bytes_read == 0 {
                    break;
                }
                chunk_bytes_read += bytes_read;
            }
            currently_read += chunk_bytes_read;
            index += 1;
        }
        self.position += currently_read as u64;
        Ok(currently_read)
    }
}

impl Seek for ChunkedFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let position: i64 = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(p) => self.size as i64 + p,
            SeekFrom::Current(p) => self.position as i64 + p,
        };
        if position < 0 {
            return Err(ErrorKind::UnexpectedEof.into());
        }
        self.position = position as u64;
        Ok(self.position)
    }
}

impl AsRawFd for ChunkedFile {
    fn as_raw_fd(&self) -> RawFd {
        let hash_concat = self
            .chunks
            .iter()
            .fold(String::new(), |mut acc, (_, chunk)| {
                acc.extend(chunk.content_hash.chars());
                acc
            });
        let hash = md5::compute(hash_concat.as_bytes()).0;
        let (int_bytes, _) = hash.as_slice().split_at(size_of::<u64>());
        u64::from_le_bytes(int_bytes.try_into().expect("Casting to u64 should work")) as RawFd
    }
}

impl FileLike for ChunkedFile {}

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
    #[error("The path is not a file")]
    NotAFile,
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

pub fn compose_object_path(object_hash: &str, hash_suffix: &str) -> PathBuf {
    let (first, second) = object_hash.split_at(2);
    Path::new("data")
        .join(first)
        .join(second.to_owned() + hash_suffix)
}
