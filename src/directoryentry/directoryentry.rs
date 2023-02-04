use std::ops::{BitAnd, ShrAssign};

use crate::common::{CvmfsError, CvmfsResult};
use crate::directoryentry::chunk::Chunk;
use crate::directoryentry::content_hash_types;
use crate::directoryentry::content_hash_types::ContentHashTypes;

#[derive(Copy, Clone)]
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
    type Output = i32;

    fn bitand(self, rhs: Flags) -> Self::Output {
        self & rhs
    }
}

impl BitAnd<i32> for Flags {
    type Output = i32;

    fn bitand(self, rhs: i32) -> Self::Output {
        self & rhs
    }
}

impl BitAnd<Flags> for i32 {
    type Output = i32;

    fn bitand(self, rhs: Flags) -> Self::Output {
        self & rhs
    }
}

pub struct PathHash {
    hash1: u64,
    hash2: u64,
}

pub struct DirectoryEntryWrapper {
    directory_entry: DirectoryEntry,
    path: String,
}

pub struct DirectoryEntry {
    md5_path_1: u64,
    md5_path_2: u64,
    parent_1: u64,
    parent_2: u64,
    content_hash: String,
    flags: i32,
    size: u32,
    mode: i32,
    mtime: u64,
    name: String,
    symlink: Option<String>,
    content_hash_type: ContentHashTypes,
    chunks: Vec<Chunk>,
}

impl DirectoryEntry {
    pub fn add_chunks(&mut self) {
        unimplemented!()
    }

    pub fn catalog_database_fields() -> &'static str {
        "md5path_1, md5path_2, parent_1, parent_2, hash, flags, size, mode, mtime, name, symlink"
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
        format!("{}{}", &self.content_hash, ContentHashTypes::to_suffix(&self.content_hash_type))
    }

    fn read_content_hash_type(flags: i32) -> ContentHashTypes {
        let mut bit_mask = Flags::ContentHashTypes as i32;
        let mut right_shifts = 0;
        while (bit_mask & 1) == 0 {
            bit_mask >>= 1;
            right_shifts += 1;
        }
        (((flags & Flags::ContentHashTypes) >> right_shifts) + 1).into()
    }
}