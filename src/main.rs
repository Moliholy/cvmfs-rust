use std::env;
use std::ffi::OsStr;

use cvmfs::fetcher::fetcher::Fetcher;
use cvmfs::file_system::CernvmFileSystem;
use cvmfs::repository::Repository;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Please specify url of the repository and the mount point");
    }
    let repo_url = &args[1];
    let mountpoint = &args[2];
    let repo_cache = if args.len() > 3 { args[3].clone() } else { "/tmp/cvmfs".into() };
    let fetcher = Fetcher::new(&repo_url, &repo_cache)
        .expect("Failure creating the fetcher");
    let repository = Repository::new(fetcher)
        .expect("Failure creating the repository");
    let file_system = CernvmFileSystem::new(repository)
        .expect("Failure creating the file system");

    let fuse_args = [OsStr::new("-o"), OsStr::new("fsname=cernvmfs")];
    fuse_mt::mount(fuse_mt::FuseMT::new(file_system, 1), &mountpoint, &fuse_args[..]).unwrap();
}
