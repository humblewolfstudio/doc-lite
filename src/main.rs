use rustyline::{error::ReadlineError, history::FileHistory, DefaultEditor, Editor};
use serde::{Deserialize, Serialize};
use std::{
    borrow::BorrowMut,
    fmt,
    fs::File,
    io::{self, Read, Write},
};

use serde_json::Value;

use bson::{from_reader, Document};

const TABLE_MAX_DOCUMENTS: usize = 100;
#[derive(Serialize, Deserialize)]
struct Database {
    tables: Option<Vec<Collection>>,
}
#[derive(Serialize, Deserialize)]
struct Collection {
    name: String,
    num_documents: usize,
    pages: Vec<Row>,
}

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Collection Name: {}", self.name)?;
        writeln!(f, "Number of Documents: {}", self.num_documents)?;
        writeln!(f, "Documents:")?;
        for (i, row) in self.pages.iter().enumerate() {
            writeln!(f, "Row {}: {:?}", i + 1, row)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Row {
    document: Document,
}

enum ExecuteResult {
    ExecuteSuccess,
    ExecuteTableFull,
    ExecuteFailed,
    ExecuteTableUndefined,
    ExecuteCollectionAlreadyExists,
}

enum PrepareResult {
    PrepareSuccess,
    PrepareUnrecognizedStatement,
    PrepareSyntaxError,
    PrepareCollectionDoesntExist,
    PrepareMissingCollection,
}

enum CollectionResult {
    CollectionSuccess,
    CollectionDoesntExist,
}

enum StatementType {
    StatementFind,
    StatementInsert,
    StatementCreate,
    StatementPeek,
}

struct Statement {
    x_type: Option<StatementType>,
    row_to_insert: Option<Row>,
    collection: String,
    collection_name: String,
}

fn main() {
    println!("Hello, world!");

    let filename = "./test.db";
    let mut database: Database = db_open(filename);
    let mut rl = DefaultEditor::new().unwrap();
    loop {
        let mut input_buffer = String::new();
        print_prompt();
        let exit = get_input(&mut rl, &mut input_buffer, &mut database);
        if exit {
            db_close(&mut database, filename);
            return;
        }
        input_buffer.clear();
    }
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().expect("Failed to flush stdout");
}

fn get_input(
    rl: &mut Editor<(), FileHistory>,
    input_buffer: &mut String,
    database: &mut Database,
) -> bool {
    let readline = rl.readline("db> ");
    match readline {
        Ok(line) => {
            let str = line.trim();

            if let Some(command) = str.chars().nth(0) {
                if command == '.' {
                    return handle_command(str);
                } else {
                    let mut statement = Statement {
                        x_type: None,
                        row_to_insert: None,
                        collection: String::new(),
                        collection_name: String::new(),
                    };

                    let prepare = prepare_statement(str, &mut statement, database);
                    match prepare {
                        PrepareResult::PrepareSuccess => {
                            match execute_statement(statement, database) {
                                ExecuteResult::ExecuteSuccess => println!("Executed."),
                                ExecuteResult::ExecuteFailed => println!("Failed."),
                                ExecuteResult::ExecuteTableFull => println!("Table full."),
                                ExecuteResult::ExecuteTableUndefined => {
                                    println!("Collection doesnt exist.")
                                }
                                ExecuteResult::ExecuteCollectionAlreadyExists => {
                                    eprintln!("Collection already exists.")
                                }
                            }
                        }
                        PrepareResult::PrepareUnrecognizedStatement => {
                            eprintln!("Unrecognized keyword at start of '{}'", str);
                        }
                        PrepareResult::PrepareSyntaxError => {
                            eprintln!("Syntax error. Could not parse statement");
                        }
                        PrepareResult::PrepareCollectionDoesntExist => {
                            eprintln!("Collection doesnt exist")
                        }
                        PrepareResult::PrepareMissingCollection => {
                            eprintln!("Collection is missing in query.")
                        }
                    }
                    return false;
                }
            }
        }
        Err(ReadlineError::Interrupted) => {
            return true;
        }
        Err(error) => {
            eprintln!("Error reading input: {}", error);
        }
    }

    return false;
}

fn handle_command(command: &str) -> bool {
    let command_parsed: Vec<&str> = command.split(' ').collect();
    let input_command = command_parsed[0];
    match input_command {
        ".exit" => {
            println!("Bye!");
            return true;
        }
        _ => {
            println!("Command '{}' not recognized", command);
        }
    }
    return false;
}

fn prepare_statement(
    input: &str,
    statement: &mut Statement,
    database: &mut Database,
) -> PrepareResult {
    let input_parsed: Vec<&str> = input.split(' ').collect();

    let statement_input = input_parsed[0];

    match statement_input {
        "insert" => {
            if input_parsed.len() < 2 {
                return PrepareResult::PrepareCollectionDoesntExist;
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

            let document = string_to_document(input_parsed[2]).unwrap();

            statement.row_to_insert = Some(Row { document: document });
            return PrepareResult::PrepareSuccess;
        }
        "find" => {
            if input_parsed.len() < 2 {
                return PrepareResult::PrepareCollectionDoesntExist;
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
        "create" => {
            if input_parsed.len() < 2 {
                return PrepareResult::PrepareCollectionDoesntExist;
            }

            let collection_name = input_parsed[1];
            statement.x_type = Some(StatementType::StatementCreate);
            statement.collection_name = collection_name.to_owned();
            return PrepareResult::PrepareSuccess;
        }
        "peek" => {
            statement.x_type = Some(StatementType::StatementPeek);
            return PrepareResult::PrepareSuccess;
        }
        _ => {
            return PrepareResult::PrepareUnrecognizedStatement;
        }
    }
}

fn get_collection(
    statement: &mut Statement,
    database: &mut Database,
    collection_name: &str,
) -> CollectionResult {
    let tables = database.tables.as_ref().unwrap();
    for item in tables.iter() {
        if item.name.eq(collection_name) {
            statement.collection = collection_name.to_owned();
            return CollectionResult::CollectionSuccess;
        }
    }

    return CollectionResult::CollectionDoesntExist;
}

fn string_to_document(string: &str) -> Result<Document, String> {
    match serde_json::from_str::<Value>(&string) {
        Ok(json_value) => {
            let bson_doc = bson::to_document(&json_value).expect("Failed");
            return Ok(bson_doc);
        }
        Err(_e) => return Err("Error parsing string".to_string()),
    }
}

fn execute_statement(statement: Statement, database: &mut Database) -> ExecuteResult {
    match &statement.x_type {
        Some(_type) => match _type {
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
        },
        None => {
            return ExecuteResult::ExecuteFailed;
        }
    }
}

fn execute_peek(database: &mut Database) -> ExecuteResult {
    let mut collections: Vec<&str> = Vec::new();

    for item in database.tables.as_ref().unwrap().iter() {
        collections.push(&item.name);
    }

    println!("{:?}", collections);

    return ExecuteResult::ExecuteSuccess;
}

fn execute_create(statement: Statement, database: &mut Database) -> ExecuteResult {
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

fn execute_insert(statement: Statement, database: &mut Database) -> ExecuteResult {
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

            let row_to_insert: Row = statement.row_to_insert.unwrap();

            collection.pages.push(row_to_insert);
            collection.num_documents += 1;
            return ExecuteResult::ExecuteSuccess;
        }
        None => return ExecuteResult::ExecuteTableUndefined,
    }
}

fn execute_find(statement: Statement, database: &mut Database) -> ExecuteResult {
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

fn db_open(filename: &str) -> Database {
    //const ARRAY_REPEAT_VALUE: Option<Row> = None;
    match database_opener(filename) {
        Ok(db) => return db,
        Err(e) => {
            eprintln!("{}", e);
            return Database {
                tables: Some(Vec::new()),
            };
        }
    }
}

fn db_close(database: &mut Database, filename: &str) {
    let document = bson::to_document(database).expect("Failed to serialize Database");

    let mut serialized_data: Vec<u8> = Vec::new();
    document
        .to_writer(&mut serialized_data)
        .expect("Failed to serialize BSON");

    match File::create(filename) {
        Ok(mut file) => {
            file.write_all(&serialized_data)
                .expect("Error writing to file");
            return;
        }
        Err(_e) => {
            println!("Error creating file");
            return;
        }
    }
}

fn database_opener(filename: &str) -> Result<Database, String> {
    match File::open(filename) {
        Ok(mut file) => {
            let mut buffer = Vec::new();
            match file.read_to_end(&mut buffer) {
                Ok(_usize) => {
                    let document = from_reader(&buffer[..]).expect("Failed to deserialize BSON");

                    return Ok(bson::from_bson::<Database>(document)
                        .expect("Failed to convert BSON to Database"));
                }
                Err(_e) => return Err("Error reading file".to_string()),
            }
        }
        Err(_e) => return Err("Error opening file".to_string()),
    }
}
