use bson::Document;

use crate::{
    bson_functions::string_to_document, collection::Collection, get_collection,
    statement::StatementType, CollectionResult, Database, ExecuteResult, PrepareResult, Statement,
    TABLE_MAX_DOCUMENTS,
};

pub fn prepare_insert(
    input_parsed: Vec<&str>,
    statement: &mut Statement,
    database: &mut Database,
) -> PrepareResult {
    if input_parsed.len() < 2 {
        return PrepareResult::PrepareMissingCollection;
    }

    let collection_name = input_parsed[1];
    match get_collection(statement, database, collection_name) {
        CollectionResult::CollectionDoesntExist => {
            return PrepareResult::PrepareCollectionDoesntExist
        }
        CollectionResult::CollectionSuccess => {}
    }

    if input_parsed.len() < 3 {
        return PrepareResult::PrepareSyntaxError;
    }

    statement.set_type(StatementType::StatementInsert);

    let json_input = input_parsed[2..].join("");

    match string_to_document(&json_input) {
        Ok(document) => {
            statement.set_row_to_insert(Document::from(document));
            return PrepareResult::PrepareSuccess;
        }
        Err(_err) => {
            return PrepareResult::PrepareCantParseJson;
        }
    }
}

pub fn execute_peek(database: &mut Database) -> ExecuteResult {
    let mut collections: Vec<String> = Vec::new();
    let mut name;
    for item in database.get_collections().iter() {
        name = item.get_name();
        collections.push(name);
    }

    println!("{:?}", collections);

    return ExecuteResult::ExecuteSuccess;
}

pub fn prepare_create(input_parsed: Vec<&str>, statement: &mut Statement) -> PrepareResult {
    if input_parsed.len() < 2 {
        return PrepareResult::PrepareMissingCollection;
    }

    let collection_name = input_parsed[1];
    statement.set_type(StatementType::StatementCreate);
    statement.set_collection_name(collection_name.to_owned());
    return PrepareResult::PrepareSuccess;
}

pub fn execute_create(statement: Statement, database: &mut Database) -> ExecuteResult {
    for item in database.get_collections().iter() {
        if item.get_name().eq(&statement.get_collection()) {
            return ExecuteResult::ExecuteCollectionAlreadyExists;
        }
    }

    let collection = Collection::new(statement.get_collection_name());

    database.add_collection(collection);

    return ExecuteResult::ExecuteSuccess;
}

pub fn execute_insert(statement: Statement, database: &mut Database) -> ExecuteResult {
    let mut table: Option<&mut Collection> = None; //TODO move a collection reference inside statement

    let collections: &mut Vec<Collection> = database.get_collections();

    for i in 0..collections.len() {
        let item = &collections[i];
        if item.get_name().eq(&statement.get_collection()) {
            table = Some(&mut collections[i]);
            break; // Exit loop once the desired item is found
        }
    }

    match table {
        Some(collection) => {
            if collection.get_num_docuents() >= TABLE_MAX_DOCUMENTS {
                return ExecuteResult::ExecuteTableFull;
            }

            let row_to_insert: Document = statement.get_row_to_insert();

            collection.add_to_collection(row_to_insert);
            return ExecuteResult::ExecuteSuccess;
        }
        None => return ExecuteResult::ExecuteTableUndefined,
    }
}

pub fn prepare_find(
    input_parsed: Vec<&str>,
    statement: &mut Statement,
    database: &mut Database,
) -> PrepareResult {
    if input_parsed.len() < 2 {
        return PrepareResult::PrepareMissingCollection;
    }

    let collection_name = input_parsed[1];
    match get_collection(statement, database, collection_name) {
        CollectionResult::CollectionDoesntExist => {
            return PrepareResult::PrepareCollectionDoesntExist
        }
        CollectionResult::CollectionSuccess => {}
    }
    let json_input = input_parsed[2..].join("");
    statement.set_type(StatementType::StatementFind);
    match string_to_document(&json_input) {
        Ok(document) => {
            statement.set_row_to_insert(Document::from(document));
            return PrepareResult::PrepareSuccess;
        }
        Err(_err) => {
            return PrepareResult::PrepareCantParseJson;
        }
    }
}

pub fn execute_find(statement: Statement, database: &mut Database) -> ExecuteResult {
    let mut table: Option<&Collection> = None; //TODO move a collection reference inside statement

    let collections: &mut Vec<Collection> = database.get_collections();

    for i in 0..collections.len() {
        let item = &collections[i];
        if item.get_name().eq(&statement.get_collection()) {
            table = Some(&mut collections[i]);
            break;
        }
    }

    match table {
        Some(collection) => {
            let documents = collection.simple_search(statement.get_row_to_insert());
            for i in documents.iter() {
                println!("{}", i);
            }
        }
        None => return ExecuteResult::ExecuteTableUndefined,
    }

    return ExecuteResult::ExecuteSuccess;
}
