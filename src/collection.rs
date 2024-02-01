use std::fmt;

use serde::Deserialize;

use crate::document::Doc;

#[derive(serde::Serialize, Deserialize, Clone)]
pub struct Collection {
    name: String,
    num_documents: usize,
    documents: Vec<Doc>,
}

pub enum CollectionResult {
    CollectionSuccess,
    CollectionDoesntExist,
}

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Collection Name: {}", self.name)?;
        writeln!(f, "Number of Documents: {}", self.num_documents)?;
        writeln!(f, "Documents:")?;
        for (i, row) in self.documents.iter().enumerate() {
            writeln!(f, "Row {}: {:?}", i + 1, row)?;
        }
        Ok(())
    }
}

impl Collection {
    pub fn new(name: String) -> Self {
        Self {
            name,
            num_documents: 0,
            documents: Vec::new(),
        }
    }

    pub fn get_name(&self) -> String {
        return self.name.to_owned();
    }

    pub fn get_num_docuents(&self) -> usize {
        return self.num_documents;
    }

    pub fn add_to_collection(&mut self, doc: Doc) {
        self.documents.push(doc);
        self.num_documents += 1;
    }
    #[allow(dead_code)]
    pub fn get_collection(&self) -> &Vec<Doc> {
        return &self.documents;
    }
}
