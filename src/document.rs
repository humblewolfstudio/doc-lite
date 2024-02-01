use bson::Document;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Doc {
    document: Document,
}

impl Doc {
    pub fn new(document: Document) -> Self {
        Self { document }
    }
}
