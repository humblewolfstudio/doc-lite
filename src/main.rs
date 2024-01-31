use queries::{
    execute_create, execute_find, execute_insert, execute_peek, prepare_create, prepare_find,
    prepare_insert,
};
use rustyline::{error::ReadlineError, history::FileHistory, DefaultEditor, Editor};
use serde::{Deserialize, Serialize};
use std::{
    env, fmt,
    fs::File,
    io::{self, Read, Write},
};

use bson::{from_reader, Document};

mod bson_functions;
mod queries;

const TABLE_MAX_DOCUMENTS: usize = 100;

#[derive(Serialize, Deserialize)]
struct Database {
    tables: Option<Vec<Collection>>,
}
#[derive(Serialize, Deserialize)]
struct Collection {
    name: String,
    num_documents: usize,
    pages: Vec<Doc>,
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
struct Doc {
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
    row_to_insert: Option<Doc>,
    collection: String,
    collection_name: String,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename;
    if args.len() > 1 {
        filename = args[1].as_str();
    } else {
        filename = "./db.docl";
    }

    let mut database: Database = db_open(filename);
    let mut rl = DefaultEditor::new().unwrap();
    loop {
        let mut input_buffer = String::new();
        print_prompt();
        let exit = get_input(&mut rl, &mut database);
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

fn get_input(rl: &mut Editor<(), FileHistory>, database: &mut Database) -> bool {
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
            return prepare_insert(input_parsed, statement, database);
        }
        "find" => {
            return prepare_find(input_parsed, statement, database);
        }
        "create" => {
            return prepare_create(input_parsed, statement);
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
                Err(_e) => return Err("Error reading database file".to_string()),
            }
        }
        Err(_e) => return Err("Database file doesnt exist".to_string()),
    }
}
