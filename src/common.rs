use reqwest::Error;

pub type CvmfsResult<R> = Result<R, CvmfsError>;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum CvmfsError {
    #[error("Invalid Certificate")]
    Certificate,
    #[error("IO error")]
    IO,
    #[error("Incomplete root file signature")]
    IncompleteRootFileSignature,
    #[error("Invalid root file signature")]
    InvalidRootFileSignature,
    #[error("Cache directory not found")]
    CacheDirectoryNotFound,
}

impl From<Error> for CvmfsError {
    fn from(_: Error) -> Self {
        CvmfsError::IO
    }
}

impl From<std::io::Error> for CvmfsError {
    fn from(_: std::io::Error) -> Self {
        CvmfsError::IO
    }
}