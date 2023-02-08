use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::mem;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, NaiveDateTime, Utc};
use fuse_mt::{CallbackResult, FileAttr, FilesystemMT, FileType, RequestInfo, ResultData, ResultEmpty, ResultEntry, ResultOpen, ResultReaddir, ResultSlice, ResultStatfs};
use fuse_mt::DirectoryEntry as FuseDirectoryEntry;
use rand::Rng;

use crate::common::CvmfsResult;
use crate::directoryentry::directoryentry::DirectoryEntry;
use crate::repository::Repository;

const TTL: Duration = Duration::from_secs(1);

fn map_dirent_type_to_fs_kind(dirent: &DirectoryEntry) -> FileType {
    if dirent.is_file() {
        FileType::RegularFile
    } else if dirent.is_directory() {
        FileType::Directory
    } else if dirent.is_symlink() {
        FileType::Symlink
    } else {
        FileType::RegularFile
    }
}

#[derive(Debug)]
pub struct CernvmFileSystem {
    repository: RwLock<Repository>,
    opened_files: RwLock<HashMap<String, File>>,
}

impl FilesystemMT for CernvmFileSystem {
    fn destroy(&self) {
        self.opened_files.write().unwrap().drain();
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
        let path = path.to_str().unwrap();
        let mut repo = self.repository.write().unwrap();
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(result) => {
                let date_time: DateTime<Utc> = DateTime::from_utc(NaiveDateTime::from_timestamp_opt(result.mtime, 0).unwrap(), Utc);
                let time = SystemTime::from(date_time);
                let file_attr = FileAttr {
                    size: result.size,
                    blocks: 1 + result.size / 512,
                    atime: time,
                    mtime: time,
                    ctime: time,
                    crtime: time,
                    kind: map_dirent_type_to_fs_kind(&result),
                    perm: (result.mode & 0o7777) as u16,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 1,
                    flags: 0,
                };
                Ok((TTL, file_attr))
            }
        }
    }

    fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
        let path = path.to_str().unwrap();
        let mut repo = self.repository.write().unwrap();
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(result) => {
                if !result.is_symlink() {
                    return Err(libc::ENOLINK);
                }
                Ok(result.symlink.unwrap().into_bytes())
            }
        }
    }

    fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        let path = path.to_str().unwrap();
        let mut repo = self.repository.write().unwrap();
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(result) => {
                if !result.is_file() {
                    return Err(libc::ENOENT);
                }
                let file = repo.get_file(path)?.unwrap();
                let fd = file.as_raw_fd() as u64;
                self.opened_files.write().unwrap().insert(path.into(), file);
                Ok((fd, 0))
            }
        }
    }

    fn read(&self, _req: RequestInfo, path: &Path, _fh: u64, offset: u64, size: u32, callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult) -> CallbackResult {
        let path = path.to_str().unwrap();
        let mut opened_files = self.opened_files.write().unwrap();
        let file = opened_files.get_mut(path.into()).unwrap();

        let mut data = Vec::<u8>::with_capacity(size as usize);
        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            return callback(Err(e.raw_os_error().unwrap()));
        }
        match file.read(unsafe { mem::transmute(data.spare_capacity_mut()) }) {
            Ok(n) => unsafe { data.set_len(n) },
            Err(e) => return callback(Err(e.raw_os_error().unwrap()))
        }

        callback(Ok(&data))
    }

    fn flush(&self, _req: RequestInfo, _path: &Path, _fh: u64, _lock_owner: u64) -> ResultEmpty {
        Ok(())
    }

    fn release(&self, _req: RequestInfo, path: &Path, _fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
        let path = path.to_str().unwrap();
        match self.opened_files.write().unwrap().remove(path.into()) {
            None => Err(libc::ENOENT),
            Some(_) => Ok(())
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        let path = path.to_str().unwrap();
        let mut repo = self.repository.write().unwrap();
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(result) => {
                if !result.is_directory() {
                    return Err(libc::ENOENT);
                }
                // don't need file descriptors if we have the path
                let mut rng = rand::thread_rng();
                let fd = rng.gen();
                Ok((fd, 0))
            }
        }
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, _fh: u64) -> ResultReaddir {
        let path = path.to_str().unwrap();
        let mut repo = self.repository.write().unwrap();
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(result) => {
                if !result.is_directory() {
                    return Err(libc::ENOENT);
                }
                let entries = repo.list_directory(path)?.into_iter().map(|dirent| {
                    FuseDirectoryEntry {
                        kind: map_dirent_type_to_fs_kind(&dirent),
                        name: OsString::from(dirent.name),
                    }
                }).collect();
                Ok(entries)
            }
        }
    }

    fn releasedir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
        Ok(())
    }

    fn access(&self, _req: RequestInfo, path: &Path, _mask: u32) -> ResultEmpty {
        let path = path.to_str().unwrap();
        let mut repo = self.repository.write().unwrap();
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(_) => Ok(())
        }
    }
}

impl CernvmFileSystem {
    pub fn new(repository: Repository) -> CvmfsResult<Self> {
        Ok(Self {
            repository: RwLock::new(repository),
            opened_files: Default::default(),
        })
    }
}