from typing import List
from .table import Table
from .column import Column
from .datatypes import IntegerType, StringType, BooleanType, DateType
import json

class Database:
    """
    A class representing a simple in-memory database.
    Attributes:
        name (str): The name of the database.
        tables (dict): A dictionary mapping table names to Table objects.
        transaction_in_progress (bool): A flag indicating whether a transaction is in progress.
    Methods:
        from_json(cls, filename: str):
        create_table(name: str, columns: List[Column]):
        drop_table(name: str):
        save(filename: str):
        __enter__():
        __exit__(exc_type, exc_value, traceback):
        commit():
        rollback():
"""
    def __init__(self, name: str):
        """
        Initialize a new Database instance.
        """
        self.name = name
        self.tables = {}
        self.transaction_in_progress = False  # Прапор для виявлення активної транзакції

    @classmethod
    def from_json(cls, filename: str):
        """
        Load a database from a JSON file.

        :param filename: The JSON file to load.
        :return: A Database instance populated with data.
        """
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)

        if "name" not in data or "tables" not in data:
            raise ValueError("Invalid database format. Missing 'name' or 'tables'.")

        db = cls(data["name"])

        # Відновлення таблиць
        for table_name, rows in data["tables"].items():
            columns = []
            for column_data in data["columns"][table_name]:
                column = Column(column_data["name"], eval(column_data["type"])())  # Динамічне створення типу
                columns.append(column)

            table = db.create_table(table_name, columns)

            for row in rows:
                table.insert(row)

        return db


    def create_table(self, name: str, columns: List[Column]):
        """
        Create a new table in the database.

        :param name: The name of the table.
        :param columns: A list of Column objects defining the table schema.
        :raises ValueError: If a table with the same name already exists.
        """
        if name in self.tables:
            raise ValueError(f"Table {name} already exists.")
        self.tables[name] = Table(name, columns)

        # Store column types for validation during data insertion
        self.tables[name].column_types = {col.name: col.data_type for col in columns}

        return self.tables[name]

    def drop_table(self, name: str):
        """
        Drop a table from the database.

        :param name: The name of the table to drop.
        :raises ValueError: If the table does not exist.
        """
        if name in self.tables:
            del self.tables[name]
        else:
            raise ValueError(f"Table {name} does not exist.")

    def save(self, filename: str):
        """
        Save the current state of the database to a JSON file.
        """
        print(self.tables)
        print(self.tables.items())  # Подивимося, що зберігається у self.tables

        data = {
            "name": self.name,  # Додаємо ім'я бази даних
            "tables": {name: [row.data for row in table.rows] for name, table in self.tables.items()}
        }
        data["columns"] = {
            name: [{"name": getattr(col, "name", col), "type": type(col.data_type).__name__}
                for col in table.columns.values()]
            for name, table in self.tables.items()
        }
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        """
        if self.transaction_in_progress:
            raise ValueError("A transaction is already in progress.")
        
        # Save the full state: columns (as a list), rows, and next_id for each table.
        self.initial_state = {
            name: (
                list(table.columns.values()),  # Save columns as a list of Column objects
                table.rows.copy(),
                table.next_id
            )
            for name, table in self.tables.items()
        }
        self.transaction_in_progress = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context related to this object.
        """
        if exc_type:
            # Rollback: reconstruct each table properly.
            for name, (columns, rows, next_id) in self.initial_state.items():
                table = Table(name, columns)  # Pass only name and columns
                table.rows = rows           # Restore rows
                table.next_id = next_id     # Restore next_id
                self.tables[name] = table
            print("Transaction aborted due to an error.")
        else:
            print("Transaction committed.")
            
        self.transaction_in_progress = False
        return False  # Allow exception propagation

    def commit(self):
        """
        Commit the current transaction.

        :raises ValueError: If no transaction is in progress.
        """
        if not self.transaction_in_progress:
            raise ValueError("No transaction in progress.")
        print("Transaction committed.")
        self.transaction_in_progress = False

    def rollback(self):
        """
        Rollback the current transaction.
        """
        if not self.transaction_in_progress:
            raise ValueError("No transaction in progress.")
        for name, (columns, rows, next_id) in self.initial_state.items():
            table = Table(name, columns)
            table.rows = rows
            table.next_id = next_id
            self.tables[name] = table
        print("Transaction rolled back.")
        self.transaction_in_progress = False

    
     # Методи CRUD для роботи з динамічними моделями
    def create(self, model_name, data):
        """
        Creates a new record for a given model. When the model (table) does not exist,
        the table is dynamically created and appropriate data types are determined based on the values.
        """
        if model_name not in self.tables:
            columns = []
            for key, value in data.items():
                # Determine the datatype based on the type of value
                if isinstance(value, int):
                    dtype = IntegerType()
                elif isinstance(value, bool):
                    dtype = BooleanType()
                elif isinstance(value, str):
                    dtype = StringType()
                columns.append(Column(key, dtype))
            self.create_table(model_name, columns)
        table = self.tables[model_name]
        new_row = table.insert(data)
        return new_row.data


    def get(self, model_name, obj_id):
        """
        Повертає запис за ID для заданої моделі.
        """
        if model_name not in self.tables:
            return None
        table = self.tables[model_name]
        row = table.get_by_id(obj_id)
        return row.data if row else None

    def update(self, model_name, obj_id, data):
        """
        Оновлює запис за ID для заданої моделі.
        """
        if model_name not in self.tables:
            return None
        table = self.tables[model_name]
        row = table.update(obj_id, data)
        return row.data if row else None

    def delete(self, model_name, obj_id):
        """
        Видаляє запис за ID для заданої моделі.
        """
        if model_name not in self.tables:
            return False
        table = self.tables[model_name]
        return table.delete(obj_id)