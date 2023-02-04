/// Wrapper around file chunks in the CVMFS catalogs
pub struct Chunk {
    offset: u32,
    size: u32,
    content_hash: String,
    content_hash_type: i32,
}

impl Chunk {
    pub fn new(offset: u32, size: u32, content_hash: String, content_hash_type: i32) -> Self {
        Self {
            offset,
            size,
            content_hash,
            content_hash_type,
        }
    }

    pub fn catalog_database_fields() -> &'static str {
        "md5path_1, md5path_2, offset, size, hash"
    }
}