use crate::{
    bson_functions::string_to_document, get_collection, Collection, CollectionResult, Database,
    Doc, ExecuteResult, PrepareResult, Statement, StatementType, TABLE_MAX_DOCUMENTS,
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

    statement.x_type = Some(StatementType::StatementInsert);

    let json_input = input_parsed[2..].join("");

    match string_to_document(&json_input) {
        Ok(document) => {
            statement.row_to_insert = Some(Doc { document: document });
            return PrepareResult::PrepareSuccess;
        },
        Err(_err) => {
            return PrepareResult::PrepareCantParseJson;
        }
    }

    
}

pub fn execute_peek(database: &mut Database) -> ExecuteResult {
    let mut collections: Vec<&str> = Vec::new();

    for item in database.tables.as_ref().unwrap().iter() {
        collections.push(&item.name);
    }

    println!("{:?}", collections);

    return ExecuteResult::ExecuteSuccess;
}

pub fn prepare_create(input_parsed: Vec<&str>, statement: &mut Statement) -> PrepareResult {
    if input_parsed.len() < 2 {
        return PrepareResult::PrepareMissingCollection;
    }

    let collection_name = input_parsed[1];
    statement.x_type = Some(StatementType::StatementCreate);
    statement.collection_name = collection_name.to_owned();
    return PrepareResult::PrepareSuccess;
}

pub fn execute_create(statement: Statement, database: &mut Database) -> ExecuteResult {
    for item in database.tables.as_mut().unwrap().iter_mut() {
        if item.name.eq(&statement.collection) {
            return ExecuteResult::ExecuteCollectionAlreadyExists;
        }
    }

    let collection = Collection {
        name: statement.collection_name,
        num_documents: 0,
        pages: Vec::new(),
    };

    database.tables.as_mut().unwrap().push(collection);

    return ExecuteResult::ExecuteSuccess;
}

pub fn execute_insert(statement: Statement, database: &mut Database) -> ExecuteResult {
    let mut table: Option<&mut Collection> = None; //TODO move a collection reference inside statement

    for item in database.tables.as_mut().unwrap().iter_mut() {
        if item.name.eq(&statement.collection) {
            table = Some(item);
        }
    }

    match table {
        Some(collection) => {
            if collection.num_documents >= TABLE_MAX_DOCUMENTS {
                return ExecuteResult::ExecuteTableFull;
            }

            let row_to_insert: Doc = statement.row_to_insert.unwrap();

            collection.pages.push(row_to_insert);
            collection.num_documents += 1;
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

    statement.x_type = Some(StatementType::StatementFind);
    return PrepareResult::PrepareSuccess;
}

pub fn execute_find(statement: Statement, database: &mut Database) -> ExecuteResult {
    let mut table: Option<&Collection> = None; //TODO move a collection reference inside statement

    for item in database.tables.as_mut().unwrap().iter_mut() {
        if item.name.eq(&statement.collection) {
            table = Some(item);
        }
    }

    match table {
        Some(collection) => {
            println!("{}", collection);
        }
        None => return ExecuteResult::ExecuteTableUndefined,
    }

    return ExecuteResult::ExecuteSuccess;
}
