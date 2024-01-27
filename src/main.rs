use std::io::{self, Write};

enum PrepareResult {
    PrepareSuccess,
    PrepareUnrecognizedStatement,
}

enum StatementType {
    StatementFind,
    StatementInsert,
    Undefined,
}

struct Statement {
    x_type: StatementType,
}

fn main() {
    println!("Hello, world!");

    loop {
        let mut input_buffer = String::new();
        print_prompt();
        let exit = get_input(&mut input_buffer);
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

fn get_input(mut input_buffer: &mut String) -> bool {
    match io::stdin().read_line(&mut input_buffer) {
        Ok(_) => {
            let str = input_buffer.trim();
            //get the point???

            if let Some(command) = str.chars().nth(0) {
                if command == '.' {
                    return handle_command(str);
                } else {
                    let mut statement = Statement {
                        x_type: StatementType::Undefined,
                    };

                    let prepare = prepare_statement(str, &mut statement);
                    if let PrepareResult::PrepareSuccess = prepare {
                        execute_statement(statement);
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

fn prepare_statement(statement_input: &str, statement: &mut Statement) -> PrepareResult {
    match statement_input {
        "insert" => {
            statement.x_type = StatementType::StatementInsert;
            return PrepareResult::PrepareSuccess;
        }
        "find" => {
            statement.x_type = StatementType::StatementFind;
            return PrepareResult::PrepareSuccess;
        }
        _ => {
            println!("Command '{}' not recognized", statement_input);
            return PrepareResult::PrepareUnrecognizedStatement;
        }
    }
}

fn execute_statement(statement: Statement) {
    match statement.x_type {
        StatementType::StatementFind => {
            println!("Not implemented");
        }
        StatementType::StatementInsert => {
            println!("Not implemented");
        }
        StatementType::Undefined => {
            println!("Statement not defined");
        }
    }
}
