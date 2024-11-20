use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::Split;

use hex::ToHex;
use sha1::{Digest, Sha1};

use crate::common::{CvmfsError, CvmfsResult};

/// Base class for CernVM-FS repository's signed 'root files'.
/// A CernVM-FS repository has essential 'root files' that have a defined name and
/// serve as entry points into the repository.
/// Namely the manifest (.cvmfspublished) and the whitelist (.cvmfswhitelist) that
/// both have class representations inheriting from RootFile and implementing the
/// abstract methods defined here.
/// Any 'root file' in CernVM-FS is a signed list of line-by-line key-value pairs
/// where the key is represented by a single character in the beginning of a line
/// directly followed by the value. The key-value part of the file is terminted
/// either by EOF or by a termination line (--) followed by a signature.
/// The signature follows directly after the termination line with a hash of the
/// key-value line content (without the termination line) followed by an \n and a
/// binary string containing the private-key signature terminated by EOF.
#[derive(Debug)]
pub struct RootFile {
    checksum: Option<String>,
    contents: String,
}

impl RootFile {
    pub fn has_signature(&self) -> bool {
        self.checksum != None
    }

    pub fn lines(&self) -> Split<char> {
        self.contents.split('\n')
    }

    pub fn new(file: &File) -> CvmfsResult<Self> {
        let mut reader = BufReader::new(file);
        let mut buffer = String::new();
        let mut contents = String::new();
        let mut checksum: Option<String> = None;
        loop {
            buffer.clear();
            let mut bytes_read = reader.read_line(&mut buffer).or(Err(CvmfsError::IO))?;
            if bytes_read == 0 {
                break;
            }
            if buffer[..2].eq("--") {
                buffer.clear();
                bytes_read = reader.read_line(&mut buffer).or(Err(CvmfsError::IO))?;
                if bytes_read != 41 {
                    return Err(CvmfsError::IO);
                }
                checksum = Some(buffer[..40].into());
                break;
            } else {
                contents.push_str(buffer.as_str())
            }
        }

        if checksum.is_some() {
            let mut hasher = Sha1::new();
            hasher.update(contents.as_bytes());
            let hash = &hasher.finalize()[..];
            let signature: String = hash.encode_hex();
            if signature.ne(checksum.as_ref().unwrap()) {
                return Err(CvmfsError::InvalidRootFileSignature);
            }
        }
        Ok(Self { checksum, contents })
    }
}
