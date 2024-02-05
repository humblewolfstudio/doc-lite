use bson::from_reader;
use collection::CollectionResult;
use database::Database;
use rustyline::{error::ReadlineError, history::FileHistory, DefaultEditor, Editor};
use statement::{execute_statement, prepare_statement, ExecuteResult, PrepareResult, Statement};
use std::{
    env,
    fs::File,
    io::{self, Read, Write},
};

mod bson_functions;
mod queries;
mod statement;
mod database;
mod collection;

const TABLE_MAX_DOCUMENTS: usize = 10000;

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
        print_prompt();
        let exit = get_input(&mut rl, &mut database);
        if exit {
            match commit_changes(&mut database) {
                Ok(ok) => println!("{}", ok),
                Err(err) => println!("{}", err),
            }
            return;
        }
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
                    let mut statement = Statement::new();

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
                                ExecuteResult::ExecuteCantSaveDatabase => {
                                    println!("Cant commit changes to database")
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
                        PrepareResult::PrepareCantParseJson => {
                            eprintln!("The JSON cant be parsed");
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

fn get_collection(
    statement: &mut Statement,
    database: &mut Database,
    collection_name: &str,
) -> CollectionResult {
    let tables = database.get_collections();
    for item in tables.iter() {
        if item.get_name().eq(collection_name) {
            statement.set_collection(collection_name.to_owned());
            return CollectionResult::CollectionSuccess;
        }
    }

    return CollectionResult::CollectionDoesntExist;
}

fn db_open(filename: &str) -> Database {
    //const ARRAY_REPEAT_VALUE: Option<Row> = None;
    match database_opener(filename) {
        Ok(db) => return db,
        Err(e) => {
            eprintln!("{}", e);
            return Database::new(filename.to_owned(), Vec::new());
        }
    }
}

fn commit_changes(database: &mut Database) -> Result<String, String> {
    let document = bson::to_document(database).expect("Failed to serialize Database");

    let mut serialized_data: Vec<u8> = Vec::new();
    match document.to_writer(&mut serialized_data) {
        Ok(_ok) => {}
        Err(_err) => return Err("Failed to serialize BSON".to_string()),
    }

    match File::create(&database.get_filename()) {
        Ok(mut file) => match file.write_all(&serialized_data) {
            Ok(_ok) => return Ok("Database saved.".to_string()),
            Err(_err) => return Err("Couldnt save database".to_string()),
        },
        Err(_e) => {
            return Err("Error creating file".to_string());
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
