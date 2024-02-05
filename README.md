# DocLite

Doclite is a local development NoSQL Document based Database. All the database is stored in a single file.

## Installation

You can run the source code
```sh
cargo run
```

or build it

```sh
cargo build
```

You can also install in your system (Unix-based system works, windows dont know) running the _installation_ file

```sh
sudo ./install.sh
```

## Usage

You can run the CLI with
```sh
doclite [path to file]
```

### Commands
- .exit -> Exit from database

### Queries
- create [table name] -> Creates a table with the specified name
- peek -> Returns the tables in that database
- insert [table name] [json] -> Inserts the json to the table specified
- find [table name] [json] -> Searchs for the specified keys/values in the table
- delete [table name] [json] -> Searchs and deletes the specified keys/values in the table
- commit -> Saves all the changes to disk
