use std::path::Path;

use rusqlite::{Connection, OpenFlags, Statement};

use crate::common::{CvmfsError, CvmfsResult};

#[derive(Debug)]
pub struct DatabaseObject {
    connection: Connection,
}

unsafe impl Sync for DatabaseObject {}

impl DatabaseObject {
    pub fn new(database_file: &str) -> CvmfsResult<Self> {
        let path = Path::new(database_file);
        let connection = Self::open_database(path)?;
        Ok(Self { connection })
    }

    fn open_database(path: &Path) -> CvmfsResult<Connection> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        Ok(Connection::open_with_flags(path, flags)?)
    }

    pub fn create_prepared_statement(&self, sql: &str) -> CvmfsResult<Statement> {
        Ok(self.connection.prepare(sql)?)
    }

    pub fn read_properties_table(&self) -> CvmfsResult<Vec<(String, String)>> {
        let mut statement = self.create_prepared_statement("SELECT key, value FROM properties;")?;
        let iterator = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        iterator
            .collect::<Result<Vec<_>, _>>()
            .map_err(CvmfsError::from)
    }
}
