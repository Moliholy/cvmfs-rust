use crate::common::CvmfsResult;
use crate::database_object::DatabaseObject;
use crate::revision_tag::{RevisionTag, SQL_QUERY_DATE, SQL_QUERY_NAME, SQL_QUERY_REVISION};

#[derive(Debug)]
pub struct History {
    pub database_object: DatabaseObject,
    pub schema: String,
    pub fqrn: String,
}

unsafe impl Sync for History {}

impl History {
    pub fn new(database_file: &str) -> CvmfsResult<Self> {
        let database_object = DatabaseObject::new(database_file)?;
        let properties = database_object.read_properties_table()?;
        let mut schema = String::new();
        let mut fqrn = String::new();
        for (key, value) in properties {
            match key.as_str() {
                "schema" => schema.push_str(&value),
                "fqrn" => fqrn.push_str(&value),
                _ => {}
            }
        }
        if schema.ne("1.0") {
            panic!("Invalid schema {}", schema);
        }
        Ok(Self {
            database_object,
            schema,
            fqrn,
        })
    }

    fn get_tag_by_query(&self, query: &str, param: &str) -> CvmfsResult<Option<RevisionTag>> {
        let mut statement = self.database_object.create_prepared_statement(query)?;
        let mut rows = statement.query([param])?;
        match rows.next()? {
            None => Ok(None),
            Some(row) => Ok(Some(RevisionTag::new(row)?)),
        }
    }

    pub fn get_tag_by_name(&self, name: &str) -> CvmfsResult<Option<RevisionTag>> {
        self.get_tag_by_query(SQL_QUERY_NAME, name)
    }

    pub fn get_tag_by_revision(&self, revision: u32) -> CvmfsResult<Option<RevisionTag>> {
        self.get_tag_by_query(SQL_QUERY_REVISION, revision.to_string().as_str())
    }

    pub fn get_tag_by_date(&self, timestamp: u64) -> CvmfsResult<Option<RevisionTag>> {
        self.get_tag_by_query(SQL_QUERY_DATE, timestamp.to_string().as_str())
    }
}
