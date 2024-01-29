use crate::{Collection, Database, Doc, ExecuteResult, Statement, TABLE_MAX_DOCUMENTS};

pub fn execute_peek(database: &mut Database) -> ExecuteResult {
    let mut collections: Vec<&str> = Vec::new();

    for item in database.tables.as_ref().unwrap().iter() {
        collections.push(&item.name);
    }

    println!("{:?}", collections);

    return ExecuteResult::ExecuteSuccess;
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