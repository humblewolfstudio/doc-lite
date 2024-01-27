use std::{
    fmt::{self},
    io::{self, Write},
};

const ID_SIZE: usize = 4;
const USERNAME_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * 100;

struct Table {
    num_rows: usize,
    pages: [Option<Row>; 100],
}
#[derive(Clone, Debug)]
struct Row {
    id: i32,
    username: String,
    email: String,
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {}, {})", self.id, self.username, self.email)
    }
}

enum ExecuteResult {
    ExecuteSuccess,
    ExecuteTableFull,
    ExecuteFailed,
}

enum PrepareResult {
    PrepareSuccess,
    PrepareUnrecognizedStatement,
    PrepareSyntaxError,
}

enum StatementType {
    StatementFind,
    StatementInsert,
}

struct Statement {
    x_type: Option<StatementType>,
    row_to_insert: Option<Row>,
}

fn main() {
    println!("Hello, world!");
    let mut table: Table = new_table();
    loop {
        let mut input_buffer = String::new();
        print_prompt();
        let exit = get_input(&mut input_buffer, &mut table);
        if exit {
            return;
        }
        input_buffer.clear();
    }
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().expect("Failed to flush stdout");
}

fn get_input(mut input_buffer: &mut String, table: &mut Table) -> bool {
    match io::stdin().read_line(&mut input_buffer) {
        Ok(_) => {
            let str = input_buffer.trim();

            if let Some(command) = str.chars().nth(0) {
                if command == '.' {
                    return handle_command(str);
                } else {
                    let mut statement = Statement {
                        x_type: None,
                        row_to_insert: None,
                    };

                    let prepare = prepare_statement(str, &mut statement);
                    match prepare {
                        PrepareResult::PrepareSuccess => {
                            match execute_statement(statement, table) {
                                ExecuteResult::ExecuteSuccess => println!("Executed."),
                                ExecuteResult::ExecuteFailed => println!("Failed."),
                                ExecuteResult::ExecuteTableFull => println!("Table full."),
                            }
                        }
                        PrepareResult::PrepareUnrecognizedStatement => {
                            eprintln!("Unrecognized keyword at start of '{}'", str)
                        }
                        PrepareResult::PrepareSyntaxError => {
                            eprintln!("Syntax error. Could not parse statement")
                        }
                    }
                    return false;
                }
            }
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

fn prepare_statement(input: &str, statement: &mut Statement) -> PrepareResult {
    let input_parsed: Vec<&str> = input.split(' ').collect();
    let statement_input = input_parsed[0];
    match statement_input {
        "insert" => {
            if input_parsed.len() < 4 {
                return PrepareResult::PrepareSyntaxError;
            }

            statement.x_type = Some(StatementType::StatementInsert);
            statement.row_to_insert = Some(Row {
                id: input_parsed[1].parse::<i32>().unwrap(), //TODO fix this unwrap
                username: input_parsed[2].to_string(),
                email: input_parsed[3].to_string(),
            });
            return PrepareResult::PrepareSuccess;
        }
        "find" => {
            statement.x_type = Some(StatementType::StatementFind);
            return PrepareResult::PrepareSuccess;
        }
        _ => {
            return PrepareResult::PrepareUnrecognizedStatement;
        }
    }
}

fn execute_statement(statement: Statement, table: &mut Table) -> ExecuteResult {
    match &statement.x_type {
        Some(_type) => match _type {
            StatementType::StatementFind => {
                return execute_find(statement, table);
            }
            StatementType::StatementInsert => {
                return execute_insert(statement, table);
            }
        },
        None => {
            return ExecuteResult::ExecuteFailed;
        }
    }
}

fn execute_insert(statement: Statement, table: &mut Table) -> ExecuteResult {
    if table.num_rows >= TABLE_MAX_ROWS {
        return ExecuteResult::ExecuteTableFull;
    }

    let row_to_insert: Row = statement.row_to_insert.unwrap();

    for (_index, row_option) in table.pages.iter_mut().enumerate() {
        if row_option.is_none() {
            *row_option = Some(row_to_insert);
            table.num_rows += 1;
            return ExecuteResult::ExecuteSuccess;
        }
    }

    return ExecuteResult::ExecuteFailed;
}

fn execute_find(_statement: Statement, table: &mut Table) -> ExecuteResult {
    for i in 0..table.num_rows {
        println!("{}", table.pages.get(i).unwrap().clone().unwrap());
    }

    return ExecuteResult::ExecuteSuccess;
}

fn new_table() -> Table {
    const ARRAY_REPEAT_VALUE: Option<Row> = None;
    let table = Table {
        num_rows: 0,
        pages: [ARRAY_REPEAT_VALUE; 100],
    };

    return table;
}
