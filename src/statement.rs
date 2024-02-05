use bson::Document;

use crate::{
    commit_changes, queries::{
        execute_create, execute_delete, execute_find, execute_insert, execute_peek, prepare_create, prepare_delete, prepare_find, prepare_insert
    }, Database
};

#[derive(Clone, Copy)]
pub enum StatementType {
    StatementUninitialized,
    StatementFind,
    StatementInsert,
    StatementCreate,
    StatementPeek,
    StatementCommit,
    StatementDelete,
}

pub enum ExecuteResult {
    ExecuteSuccess,
    ExecuteTableFull,
    ExecuteFailed,
    ExecuteTableUndefined,
    ExecuteCollectionAlreadyExists,
    ExecuteCantSaveDatabase,
}

pub enum PrepareResult {
    PrepareSuccess,
    PrepareUnrecognizedStatement,
    PrepareSyntaxError,
    PrepareCollectionDoesntExist,
    PrepareMissingCollection,
    PrepareCantParseJson,
}

pub struct Statement {
    x_type: StatementType,
    row_to_insert: Option<Document>,
    collection: String,
    collection_name: String,
}

impl Statement {
    pub fn new() -> Self {
        Self {
            x_type: StatementType::StatementUninitialized,
            row_to_insert: None,
            collection: String::new(),
            collection_name: String::new(),
        }
    }

    pub fn set_type(&mut self, t: StatementType) {
        self.x_type = t;
    }

    pub fn get_type(&self) -> StatementType {
        return self.x_type;
    }

    pub fn get_collection(&self) -> String {
        return self.collection.to_owned();
    }

    pub fn set_collection(&mut self, collection: String) {
        self.collection = collection;
    }

    pub fn get_collection_name(&self) -> String {
        return self.collection_name.to_owned();
    }

    pub fn set_collection_name(&mut self, collection_name: String) {
        self.collection_name = collection_name;
    }

    pub fn get_row_to_insert(&self) -> Document {
        match &self.row_to_insert {
            Some(doc) => return doc.to_owned(),
            None => return Document::new(),
        }
    }

    pub fn set_row_to_insert(&mut self, row: Document) {
        self.row_to_insert = Some(row);
    }
}

pub fn execute_statement(statement: Statement, database: &mut Database) -> ExecuteResult {
    match &statement.get_type() {
        StatementType::StatementFind => {
            return execute_find(statement, database);
        }
        StatementType::StatementInsert => {
            return execute_insert(statement, database);
        }
        StatementType::StatementCreate => {
            return execute_create(statement, database);
        }
        StatementType::StatementPeek => {
            return execute_peek(database);
        }
        StatementType::StatementCommit => match commit_changes(database) {
            Ok(ok) => {
                println!("{}", ok);
                return ExecuteResult::ExecuteSuccess;
            }
            Err(err) => {
                println!("{}", err);
                return ExecuteResult::ExecuteCantSaveDatabase;
            }
        },
        StatementType::StatementDelete => {
            return execute_delete(statement, database);
        },
        StatementType::StatementUninitialized => {
            eprintln!("No statement ready for execution");
            return ExecuteResult::ExecuteFailed;
        }
    }
}

pub fn prepare_statement(
    input: &str,
    statement: &mut Statement,
    database: &mut Database,
) -> PrepareResult {
    let input_parsed: Vec<&str> = input.split(' ').collect();

    let statement_input = input_parsed[0];

    match statement_input {
        "insert" => {
            return prepare_insert(input_parsed, statement, database);
        }
        "find" => {
            return prepare_find(input_parsed, statement, database);
        }
        "create" => {
            return prepare_create(input_parsed, statement);
        }
        "peek" => {
            statement.set_type(StatementType::StatementPeek);
            return PrepareResult::PrepareSuccess;
        }
        "commit" => {
            statement.set_type(StatementType::StatementCommit);
            return PrepareResult::PrepareSuccess;
        }
        "delete" => {
            return prepare_delete(input_parsed, statement, database);
        }
        _ => {
            return PrepareResult::PrepareUnrecognizedStatement;
        }
    }
}
