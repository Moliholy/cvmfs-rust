use std::fs;

use cvmfs::common::CvmfsResult;
use cvmfs::fetcher::Fetcher;
use cvmfs::repository::Repository;

const TEST_CACHE_PATH: &str = "/tmp/cvmfs_test_cache";

fn setup() {
    fs::create_dir_all(TEST_CACHE_PATH).expect("Failure creating the cache");
}

#[test]
fn test_initialization() -> CvmfsResult<()> {
    setup();
    let fetcher = Fetcher::new("http://cvmfs-stratum-one.cern.ch/opt/boss", TEST_CACHE_PATH)?;
    let mut repo = Repository::new(fetcher)?;
    assert_eq!(0, repo.opened_catalogs.len());
    assert_eq!("boss.cern.ch", repo.fqrn);
    repo.retrieve_current_root_catalog()?;
    Ok(())
}
