use bson::Document;

use serde_json::Value;

pub fn string_to_document(string: &str) -> Result<Document, String> {
    match serde_json::from_str::<Value>(&string) {
        Ok(json_value) => {
            let bson_doc = bson::to_document(&json_value).expect("Failed");
            return Ok(bson_doc);
        }
        Err(_e) => return Err("Error parsing string".to_string()),
    }
}
