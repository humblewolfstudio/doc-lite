use crate::collection::Collection;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Database {
    filename: String,
    collections: Vec<Collection>,
}

impl Database {
    pub fn new(filename: String, tables: Vec<Collection>) -> Self {
        Self { filename, collections: tables }
    }

    pub fn get_filename(&self) -> String {
        return self.filename.to_owned();
    }

    pub fn get_collections(&self) -> Vec<Collection> {
        return self.collections.clone();
    }

    pub fn add_collection(&mut self, table: Collection) {
        self.collections.push(table);
    }
}
