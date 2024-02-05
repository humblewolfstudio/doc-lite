use std::fmt;

use bson::Document;
use serde::Deserialize;

#[derive(serde::Serialize, Deserialize, Clone)]
pub struct Collection {
    name: String,
    num_documents: usize,
    documents: Vec<Document>,
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

    pub fn add_to_collection(&mut self, doc: Document) {
        self.documents.push(doc);
        self.num_documents += 1;
    }
    #[allow(dead_code)]
    pub fn get_collection(&self) -> &Vec<Document> {
        return &self.documents;
    }

    pub fn simple_search(&self, query: Document) -> Vec<Document> {
        let mut sorted: Vec<Document> = Vec::new();
        for doc in self.documents.iter() {
            let mut has_keys = true;
            for (key, value) in query.iter() {
                if !doc.contains_key(key) {
                    has_keys = false;
                    break;
                }
                if doc.get(key).ne(&Some(value)) {
                    has_keys = false;
                    break;
                }
            }
            if has_keys {
                sorted.push(doc.clone());
            }
        }

        return sorted;
    }

    pub fn simple_delete(&mut self, query: Document) {
        self.documents.retain(|doc| {
            let mut has_keys = true;
            for (key, value) in query.iter() {
                if !doc.contains_key(key) || doc.get(key) != Some(value) {
                    has_keys = false;
                    break;
                }
            }
            !has_keys
        });
    }

    pub fn get_sorted_collection(&self, query: Document) -> Vec<Document> {
        let mut sorted: Vec<Document> = Vec::new();
        for doc in self.documents.iter() {
            let mut has_keys = true;
            for key in query.keys() {
                if !doc.contains_key(key) {
                    has_keys = false;
                    break;
                }
            }
            if has_keys {
                sorted.push(doc.clone());
            }
        }

        return sorted;
    }
}
