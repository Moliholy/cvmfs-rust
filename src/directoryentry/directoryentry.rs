use std::ops::BitAnd;

use hex::ToHex;
use rusqlite::{Row, Rows};

use crate::common::{CvmfsError, CvmfsResult};
use crate::directoryentry::content_hash_types::ContentHashTypes;

#[derive(Debug, Copy, Clone)]
pub enum Flags {
    Directory = 1,
    NestedCatalogMountpoint = 2,
    File = 4,
    Link = 8,
    FileStat = 16,
    NestedCatalogRoot = 32,
    FileChunk = 64,
    ContentHashTypes = 256 + 512 + 1024,
}

impl BitAnd<Flags> for Flags {
    type Output = u32;

    fn bitand(self, rhs: Flags) -> Self::Output {
        self as u32 & rhs
    }
}

impl BitAnd<u32> for Flags {
    type Output = u32;

    fn bitand(self, rhs: u32) -> Self::Output {
        self as u32 & rhs
    }
}

impl BitAnd<Flags> for u32 {
    type Output = u32;

    fn bitand(self, rhs: Flags) -> Self::Output {
        self & rhs as u32
    }
}

/// Wrapper around file chunks in the CVMFS catalogs
#[derive(Debug)]
pub struct Chunk {
    pub(crate) offset: u32,
    pub(crate) size: u32,
    pub(crate) content_hash: String,
    pub(crate) content_hash_type: ContentHashTypes,
}

#[derive(Debug)]
pub struct PathHash {
    pub(crate) hash1: u64,
    pub(crate) hash2: u64,
}

#[derive(Debug)]
pub struct DirectoryEntryWrapper {
    directory_entry: DirectoryEntry,
    path: String,
}

#[derive(Debug)]
pub struct DirectoryEntry {
    pub(crate) md5_path_1: u64,
    pub(crate) md5_path_2: u64,
    pub(crate) parent_1: u64,
    pub(crate) parent_2: u64,
    pub(crate) content_hash: Option<String>,
    pub(crate) flags: u32,
    pub(crate) size: u64,
    pub(crate) mode: u16,
    pub(crate) mtime: i64,
    pub(crate) name: String,
    pub(crate) symlink: Option<String>,
    pub(crate) content_hash_type: ContentHashTypes,
    pub(crate) chunks: Vec<Chunk>,
}

impl DirectoryEntry {
    pub fn new(row: &Row) -> CvmfsResult<Self> {
        let content_hash: Option<Vec<u8>> = row.get(4)?;
        let flags = row.get(5)?;
        Ok(Self {
            md5_path_1: row.get(0)?,
            md5_path_2: row.get(1)?,
            parent_1: row.get(2)?,
            parent_2: row.get(3)?,
            content_hash: match content_hash {
                None => None,
                Some(value) => Some(value.encode_hex())
            },
            flags,
            size: row.get(6)?,
            mode: row.get(7)?,
            mtime: row.get(8)?,
            name: row.get(9)?,
            symlink: row.get(10)?,
            content_hash_type: Self::read_content_hash_type(flags),
            chunks: vec![],
        })
    }

    pub fn add_chunks(&mut self, mut rows: Rows) -> CvmfsResult<()> {
        self.chunks.clear();
        loop {
            match rows.next() {
                Ok(row) => {
                    if let Some(row) = row {
                        self.chunks.push(Chunk {
                            offset: row.get(0)?,
                            size: row.get(1)?,
                            content_hash: row.get(2)?,
                            content_hash_type: self.content_hash_type.clone(),
                        })
                    } else {
                        break;
                    }
                }
                Err(_) => return Err(CvmfsError::DatabaseError)
            }
        }
        Ok(())
    }

    pub fn is_directory(&self) -> bool {
        self.flags & Flags::Directory > 0
    }

    pub fn is_nested_catalog_mountpoint(&self) -> bool {
        self.flags & Flags::NestedCatalogMountpoint > 0
    }

    pub fn is_nested_catalog_root(&self) -> bool {
        self.flags & Flags::NestedCatalogRoot > 0
    }

    pub fn is_file(&self) -> bool {
        self.flags & Flags::File > 0
    }

    pub fn is_symlink(&self) -> bool {
        self.flags & Flags::Link > 0
    }

    pub fn path_hash(&self) -> PathHash {
        PathHash {
            hash1: self.md5_path_1,
            hash2: self.md5_path_2,
        }
    }

    pub fn parent_hash(&self) -> PathHash {
        PathHash {
            hash1: self.parent_1,
            hash2: self.parent_2,
        }
    }

    pub fn has_chunks(&self) -> bool {
        !self.chunks.is_empty()
    }

    pub fn content_hash_string(&self) -> String {
        match &self.content_hash {
            None => String::new(),
            Some(value) => format!("{}{}", &value, ContentHashTypes::to_suffix(&self.content_hash_type))
        }
    }

    fn read_content_hash_type(flags: u32) -> ContentHashTypes {
        let mut bit_mask = Flags::ContentHashTypes as u32;
        let mut right_shifts = 0;
        while (bit_mask & 1) == 0 {
            bit_mask >>= 1;
            right_shifts += 1;
        }
        (((flags & Flags::ContentHashTypes) >> right_shifts) + 1).into()
    }
}