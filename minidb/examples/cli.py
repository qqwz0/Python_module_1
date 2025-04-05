import sys
import json
from minidb.database import Database
from minidb.column import Column
from minidb.datatypes import IntegerType, StringType, BooleanType, DateType
from minidb.query import SimpleQuery, JoinedTable

DB_FILE = "mydb.json"
db=None

def get_data_type(type_str: str):
    """Returns the corresponding data type for a string value."""
    type_str = type_str.lower()
    if type_str in ["int", "integer"]:
        return IntegerType()
    elif type_str in ["str", "string"]:
        return StringType(max_length=255)
    elif type_str in ["bool", "boolean"]:
        return BooleanType()
    elif type_str == "date":
        return DateType()
    else:
        raise ValueError(f"Unknown type: {type_str}")

def print_help():
    print("Available commands:")
    print("  create_db <db_name>")
    print("  create_table <table_name> <column1>:<type>:<nullable> <column2>:<type>:<nullable> ...")
    print("  drop_table <table_name>")
    print("  insert <table_name> <column1>=<value1> <column2>=<value2> ...")
    print("  select <table_name> [where <column> <operator> <value>] [order_by <column>]")
    print("  join <table1> <table2> on <column>")
    print("  save - Save database to file")
    print("  load - Load database from file")
    print("  help")
    print("  exit")

def load_database(file_name=DB_FILE):
    """Loads the database from a JSON file if it exists."""
    try:
        db = Database.from_json(file_name)
        print(f"Database '{file_name}' loaded successfully.")
        return db, file_name
    except FileNotFoundError:
        print(f"No existing database found: '{file_name}'. Creating a new one.")
        return Database(file_name.replace(".json", "")), file_name
    except Exception as e:
        print(f"Error loading database: {e}")
        return Database("mydb"), DB_FILE

def save_database(db, file_name=None):
    if file_name is None:
        file_name = DB_FILE
    try:
        db.save(file_name)
        print(f"Database saved to '{file_name}'.")
    except Exception as e:
        print(f"Error saving database: {e}")


def main():
    """
    Entry point for the MiniDB CLI application. This function initializes the database,
    starts the command-line interface, and processes user commands in a loop.
    Commands:
        exit: Exit the program and save the database.
        help: Display help information.
        save: Save the current state of the database.
        load <db_name>: Load a specified database file.
        create_db <db_name>: Create a new database and switch to it.
        create_table <table_name> <column1>:<type>:<nullable> ...: Create a new table with specified columns.
        drop_table <table_name>: Drop a specified table.
        insert <table_name> <column1>=<value1> <column2>=<value2> ...: Insert a new row into a specified table.
        select <table_name> [where <column> <operator> <value>] [order_by <column>]: Select and display rows from a table.
        join <table1> <table2> on <column>: Perform a join operation between two tables on a specified column.
        show_table <table_name>: Display the structure and contents of a specified table.
        show_db: Display all tables in the current database and their row counts.
    Raises:
        Exception: If any error occurs during command processing.
    """
    global db, DB_FILE  # Оголошення глобальних змінних на початку функції
    db, DB_FILE = load_database()
    print(f"MiniDB CLI started. Using database: {DB_FILE}. Type 'help' for commands.\n")

    while True:
        try:
            command = input(f"{DB_FILE}> ").strip()
            if not command:
                continue
            tokens = command.split()
            cmd = tokens[0].lower()

            if cmd == "exit":
                print("Exiting the program.")
                save_database(db)
                break

            elif cmd == "help":
                print_help()

            elif cmd == "save":
                save_database(db)

            elif cmd == "load":
                if len(tokens) < 2:
                    print("Usage: load <db_name>")
                    continue

                db_file = f"{tokens[1]}.json"
                db, DB_FILE = load_database(db_file)
                print(f"Switched to database '{DB_FILE}'.")

            elif cmd == "create_db":
                if len(tokens) < 2:
                    print("Usage: create_db <db_name>")
                    continue
                db_file = f"{tokens[1]}.json"
                
                db = Database(tokens[1])
                save_database(db, db_file)
                
                DB_FILE = db_file
                print(f"New database '{tokens[1]}' created and switched to.")

            elif cmd == "create_table":
                if len(tokens) < 3:
                    print("Usage: create_table <table_name> <column1>:<type>:<nullable> ...")
                    continue
                table_name = tokens[1]
                columns = []
                for col_def in tokens[2:]:
                    try:
                        col_name, col_type, col_nullable = col_def.split(":")
                        col_nullable = col_nullable.lower() == "true"
                        data_type = get_data_type(col_type)
                        columns.append(Column(col_name, data_type, nullable=col_nullable))
                    except Exception as e:
                        print(f"Error in column definition '{col_def}': {e}")
                        break
                else:
                    db.create_table(table_name, columns)
                    save_database(db)
                    print(f"Table '{table_name}' created.")

            elif cmd == "drop_table":
                if len(tokens) < 2:
                    print("Usage: drop_table <table_name>")
                    continue
                table_name = tokens[1]
                try:
                    db.drop_table(table_name)
                    save_database(db)
                    print(f"Table '{table_name}' dropped.")
                except ValueError as e:
                    print(e)

            elif cmd == "insert":
                if len(tokens) < 3:
                    print("Usage: insert <table_name> <column1>=<value1> <column2>=<value2> ...")
                    continue
                table_name = tokens[1]
                if table_name not in db.tables:
                    print(f"Table '{table_name}' does not exist.")
                    continue

                row_data = {}
                for pair in tokens[2:]:
                    try:
                        key, value = pair.split("=")
                        if value.isdigit():
                            value = int(value)
                        elif value.lower() in ["true", "false"]:
                            value = value.lower() == "true"
                        row_data[key] = value
                    except Exception as e:
                        print(f"Error in pair '{pair}': {e}")
                        break
                else:
                    db.tables[table_name].insert(row_data)
                    save_database(db)
                    print(f"Data inserted into table '{table_name}'.")

            elif cmd == "select":
                if len(tokens) < 2:
                    print("Usage: select <table_name> [where <column> <operator> <value>] [order_by <column>]")
                    continue
                table_name = tokens[1]
                if table_name not in db.tables:
                    print(f"Table '{table_name}' does not exist.")
                    continue

                where_clause = None
                order_by = None
                if "where" in tokens:
                    idx = tokens.index("where")
                    if len(tokens) > idx + 3:
                        where_clause = tokens[idx+1:idx+4]
                    else:
                        print("Invalid where clause format.")
                        continue
                if "order_by" in tokens:
                    idx = tokens.index("order_by")
                    if len(tokens) > idx + 1:
                        order_by = tokens[idx+1]
                    else:
                        print("Invalid order_by format.")
                        continue

                query = SimpleQuery(db.tables[table_name])
                if where_clause:
                    col, op, val = where_clause
                    if val.isdigit():
                        val = int(val)
                    elif val.lower() in ["true", "false"]:
                        val = val.lower() == "true"
                    query = query.where(col, op, val)
                if order_by:
                    query = query.order_by(order_by)
                
                results = query.execute()
                print("Query results:")
                for row in results:
                    print(row.data)

            elif cmd == "join":
                if len(tokens) != 5 or tokens[3].lower() != "on":
                    print("Usage: join <table1> <table2> on <column>")
                    continue
                
                table1_name, table2_name, join_column = tokens[1], tokens[2], tokens[4]

                if table1_name not in db.tables or table2_name not in db.tables:
                    print(f"One or both tables '{table1_name}' or '{table2_name}' do not exist.")
                    continue

                table1 = db.tables[table1_name]
                table2 = db.tables[table2_name]

                print(f"Joining {table1_name} with {table2_name} on column '{join_column}'")
                
                joined_table = JoinedTable(table1, table2, join_column)
                
                print("Join results:")
                has_data = False
                for row in joined_table:
                    print(row.data)
                    has_data = True
                
                if not has_data:
                    print("[No matching data found]")

            elif cmd == "show_table":
                if len(tokens) < 2:
                    print("Usage: show_table <table_name>")
                    continue
                table_name = tokens[1]
                if table_name not in db.tables:
                    print(f"Table '{table_name}' does not exist.")
                    continue
                table = db.tables[table_name]
                print(f"Table: {table_name}")
                print("Columns:")
                for col in table.columns:
                    # If col is a Column object, print its attributes; if it's a string, just print it.
                    if hasattr(col, 'name'):
                        print(f"  {col.name} - {col.data_type.__class__.__name__} - nullable: {col.nullable}")
                    else:
                        print(f"  {col}")
                print("Rows:")
                query = SimpleQuery(table)
                results = query.execute()
                for row in results:
                    print(row.data)

            elif cmd == "show_db":
                if not db.tables:
                    print("Database is empty. No tables found.")
                    continue
                print("Tables in database:")
                for table_name, table in db.tables.items():
                    query = SimpleQuery(table)
                    results = query.execute()
                    print(f"  {table_name}: {len(results)} row(s)")

            else:
                print("Unknown command. Type 'help' for a list of commands.")

        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
