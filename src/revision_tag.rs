use rusqlite::Row;

use crate::common::CvmfsResult;

pub const SQL_QUERY_ALL: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
ORDER BY timestamp DESC";

pub const SQL_QUERY_NAME: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
WHERE name = ? \
LIMIT 1";

pub const SQL_QUERY_REVISION: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
WHERE revision = ? \
LIMIT 1";

pub const SQL_QUERY_DATE: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
WHERE timestamp > ? \
ORDER BY timestamp ASC \
LIMIT 1";

#[derive(Debug, Clone)]
pub struct RevisionTag {
    pub name: String,
    pub hash: String,
    pub revision: i32,
    pub timestamp: u64,
    pub channel: i32,
    pub description: String,
}

impl RevisionTag {
    pub fn new(row: &Row) -> CvmfsResult<Self> {
        Ok(Self {
            name: row.get(0)?,
            hash: row.get(1)?,
            revision: row.get(2)?,
            timestamp: row.get(3)?,
            channel: row.get(4)?,
            description: row.get(5)?,
        })
    }
}
