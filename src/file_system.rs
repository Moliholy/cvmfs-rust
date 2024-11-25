use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::mem;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use fuse_mt::{
    CallbackResult, FileAttr, FileType, FilesystemMT, RequestInfo, ResultData, ResultEmpty,
    ResultEntry, ResultOpen, ResultReaddir, ResultSlice, ResultXattr,
};
use fuse_mt::{DirectoryEntry as FuseDirectoryEntry, ResultStatfs, Statfs};
use rand::Rng;

use crate::common::{CvmfsError, CvmfsResult};
use crate::directory_entry::DirectoryEntry;
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
        if let Ok(mut f) = self.opened_files.write() {
            f.drain();
        };
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
        let path = path.to_str().ok_or(CvmfsError::FileNotFound)?;
        log::info!("Getting attribute of path: {path}");
        let mut repo = self
            .repository
            .write()
            .map_err(|e| CvmfsError::Generic(format!("{:?}", e)))?;
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(result) => {
                let date_time: DateTime<Utc> = DateTime::from_timestamp(result.mtime, 0)
                    .ok_or(CvmfsError::InvalidTimestamp)?;
                let time = SystemTime::from(date_time);
                let file_attr = FileAttr {
                    size: result.size,
                    blocks: 1 + result.size / 512,
                    atime: time,
                    mtime: time,
                    ctime: time,
                    crtime: time,
                    kind: map_dirent_type_to_fs_kind(&result),
                    perm: result.mode & 0o7777,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 1,
                    flags: result.flags,
                };
                Ok((TTL, file_attr))
            }
        }
    }

    fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
        let path = path.to_str().ok_or(CvmfsError::FileNotFound)?;
        log::info!("Reading link: {path}");
        let mut repo = self.repository.write().map_err(|_| CvmfsError::Sync)?;
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(result) => {
                if !result.is_symlink() {
                    return Err(libc::ENOLINK);
                }
                Ok(result.symlink.ok_or(CvmfsError::FileNotFound)?.into_bytes())
            }
        }
    }

    fn open(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        let path = path.to_str().ok_or(CvmfsError::FileNotFound)?;
        log::info!("Opening file: {path}");
        let mut repo = self.repository.write().map_err(|_| CvmfsError::Sync)?;
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(result) => {
                if !result.is_file() {
                    return Err(libc::ENOENT);
                }
                let file = repo
                    .get_file(path)?
                    .ok_or(CvmfsError::Generic("File not found".to_string()))?;
                let fd = file.as_raw_fd() as u64;
                self.opened_files
                    .write()
                    .map_err(|_| CvmfsError::Sync)?
                    .insert(path.into(), file);
                Ok((fd, 0))
            }
        }
    }

    fn read(
        &self,
        _req: RequestInfo,
        path: &Path,
        _fh: u64,
        offset: u64,
        size: u32,
        callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult,
    ) -> CallbackResult {
        let path = match path.to_str() {
            Some(p) => p,
            None => return callback(Err(libc::ENOENT)),
        };
        log::info!("Reading file: {path}");
        let mut opened_files = match self.opened_files.write() {
            Ok(guard) => guard,
            Err(e) => {
                log::error!("{:?}", e);
                return callback(Err(libc::EIO));
            }
        };
        let file = match opened_files.get_mut(path) {
            Some(f) => f,
            None => return callback(Err(libc::ENOENT)),
        };

        let mut data = Vec::<u8>::with_capacity(size as usize);
        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            log::error!("{:?}", e);
            return callback(Err(match e.raw_os_error() {
                Some(code) => code,
                None => libc::EIO,
            }));
        }
        match file.read(unsafe {
            mem::transmute::<&mut [std::mem::MaybeUninit<u8>], &mut [u8]>(data.spare_capacity_mut())
        }) {
            Ok(n) => unsafe { data.set_len(n) },
            Err(e) => {
                log::error!("{:?}", e);
                return callback(Err(match e.raw_os_error() {
                    Some(code) => code,
                    None => libc::EIO,
                }));
            }
        }

        callback(Ok(&data))
    }

    fn flush(&self, _req: RequestInfo, path: &Path, _fh: u64, _lock_owner: u64) -> ResultEmpty {
        let path = path.to_str().ok_or(libc::ENOENT)?;
        log::info!("Flushing file: {path}");
        Ok(())
    }

    fn release(
        &self,
        _req: RequestInfo,
        path: &Path,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> ResultEmpty {
        let path = path.to_str().ok_or(libc::ENOENT)?;
        log::info!("Releasing: {path}");
        match self
            .opened_files
            .write()
            .map_err(|e| {
                log::error!("{:?}", e);
                libc::EIO
            })?
            .remove(path)
        {
            None => Err(libc::ENOENT),
            Some(_) => Ok(()),
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        let path = path.to_str().ok_or(libc::ENOENT)?;
        log::info!("Opening directory: {path}");
        let mut repo = match self.repository.write() {
            Ok(repo) => repo,
            Err(e) => {
                log::error!("{:?}", e);
                return Err(libc::EIO);
            }
        };
        match repo.lookup(path)? {
            None => {
                log::error!("Path not found: {path}");
                Err(libc::ENOENT)
            },
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
        let path = path.to_str().ok_or(libc::ENOENT)?;
        log::info!("Reading directory: {path}");
        let mut repo = self.repository.write().map_err(|_| libc::EIO)?;
        match repo.lookup(path)? {
            None => {
                log::error!("File not found: {path}");
                Err(libc::ENOENT)
            },
            Some(result) => {
                if !result.is_directory() {
                    log::error!("Path '{path}' is not a directory");
                    return Err(libc::ENOENT);
                }
                match repo.list_directory(path) {
                    Ok(entries) => Ok(entries.into_iter()
                        .map(|dirent| FuseDirectoryEntry {
                            kind: map_dirent_type_to_fs_kind(&dirent),
                            name: OsString::from(dirent.name),
                        })
                        .collect()),
                    Err(e) => {
                        log::error!("Could not list directory {path}: {:?}", e);
                        Err(e.into())
                    },
                }
            }
        }
    }

    fn releasedir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
        Ok(())
    }

    fn statfs(&self, _req: RequestInfo, _path: &Path) -> ResultStatfs {
        log::info!("Getting FS statistics");
        let mut repo = self.repository.write().map_err(|_| libc::EIO)?;
        let statistics = repo.get_statistics()?;
        Ok(Statfs {
            blocks: 1 + statistics.file_size / 512,
            bfree: 0,
            bavail: 0,
            files: statistics.regular,
            ffree: 0,
            bsize: 512,
            namelen: 255,
            frsize: 512,
        })
    }

    fn getxattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr, _size: u32) -> ResultXattr {
        Err(libc::ENODATA)
    }

    fn access(&self, _req: RequestInfo, path: &Path, _mask: u32) -> ResultEmpty {
        let path = path.to_str().ok_or(libc::ENOENT)?;
        log::info!("Accessing: {path}");
        let mut repo = self.repository.write().map_err(|_| libc::EIO)?;
        match repo.lookup(path)? {
            None => Err(libc::ENOENT),
            Some(_) => Ok(()),
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
