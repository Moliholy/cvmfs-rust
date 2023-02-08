/// Enumeration of supported content hash types
#[derive(Debug, Copy, Clone)]
pub enum ContentHashTypes {
    Unknown = -1,
    Sha1 = 1,
    Ripemd160 = 2,
    UpperBound = 3,
}

impl ContentHashTypes {
    /// Figures out the hash suffix in CVMFS's CAS
    pub fn to_suffix(obj: &Self) -> String {
        match obj {
            ContentHashTypes::Ripemd160 => "-rmd160".into(),
            _ => "".into()
        }
    }
}

impl From<u32> for ContentHashTypes {
    fn from(value: u32) -> Self {
        match value {
            1 => ContentHashTypes::Sha1,
            2 => ContentHashTypes::Ripemd160,
            3 => ContentHashTypes::UpperBound,
            _ => ContentHashTypes::Unknown,
        }
    }
}