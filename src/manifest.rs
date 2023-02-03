use chrono::{DateTime, NaiveDateTime, Utc};

use crate::rootfile::RootFile;

/**
 * Wraps information from .cvmfspublished
 */
pub struct Manifest {
    pub root_file: RootFile,
    pub root_catalog: String,
    pub root_hash: String,
    pub root_catalog_size: u32,
    pub certificate: String,
    pub history_database: String,
    pub last_modified: DateTime<Utc>,
    pub ttl: u32,
    pub revision: u32,
    pub repository_name: String,
    pub micro_catalog: String,
    pub garbage_collectable: bool,
    pub allows_alternative_name: bool,
}

impl Manifest {
    fn parse_boolean(value: &str) -> bool {
        match value {
            "yes" => true,
            "no" => false,
            _ => panic!("Invalid boolean value: {}", value)
        }
    }

    pub fn new(root_file: RootFile) -> Self {
        let mut root_catalog = String::new();
        let mut root_hash = String::new();
        let mut root_catalog_size = 0;
        let mut certificate = String::new();
        let mut history_database = String::new();
        let mut last_modified = DateTime::default();
        let mut ttl = 0;
        let mut revision = 0;
        let mut repository_name = String::new();
        let mut micro_catalog = String::new();
        let mut garbage_collectable = false;
        let mut allows_alternative_name = false;

        for line in root_file.lines() {
            let key = line.chars().next().unwrap();
            let value = &line[1..];
            match key {
                'C' => root_catalog = value.into(),
                'R' => root_hash = value.into(),
                'B' => root_catalog_size = value.parse().unwrap(),
                'X' => certificate = value.into(),
                'H' => history_database = value.into(),
                'T' => last_modified = DateTime::from_utc(NaiveDateTime::from_timestamp_millis(value.parse().unwrap()).unwrap(), Utc),
                'D' => ttl = value.parse().unwrap(),
                'S' => revision = value.parse().unwrap(),
                'N' => repository_name = value.into(),
                'L' => micro_catalog = value.into(),
                'G' => garbage_collectable = Self::parse_boolean(value),
                'A' => allows_alternative_name = Self::parse_boolean(value),
                _ => {}
            }
        }

        Self {
            root_file,
            root_catalog,
            root_hash,
            root_catalog_size,
            certificate,
            history_database,
            last_modified,
            ttl,
            revision,
            repository_name,
            micro_catalog,
            garbage_collectable,
            allows_alternative_name,
        }
    }
}