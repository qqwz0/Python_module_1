from typing import Any, Dict, List, Union
from .column import Column
from .row import Row

class Table:
    """
    Represents a table in a mini database.

    Attributes:
        name (str): The name of the table.
        columns (Dict[str, Column]): A dictionary mapping column names to Column objects.
        rows (List[Row]): A list of rows in the table.
        next_id (int): The ID to be assigned to the next inserted row.
    Methods:
        insert(row_data: Dict[str, Any]) -> Row:
        get_row(index: int) -> Union[Row, None]:
        get_by_id(row_id: int) -> Union[Row, None]:
        update(row_id: int, row_data: Dict[str, Any]) -> Union[Row, None]:
        delete(row_id: int) -> bool:
        __len__() -> int:
        __iter__() -> Iterator[Row]:
        __repr__() -> str:
        
    """

    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = {column.name: column for column in columns}
        self.rows = []
        self.next_id = 1

    def insert(self, row_data: Dict[str, Any]) -> Row:
        """
        Inserts a new row into the table.
        """
        for column_name, column in self.columns.items():
            value = row_data.get(column_name)
            if value is not None and not column.data_type.validate(value):
                raise ValueError(f"Invalid value for column {column_name}: {value}")

        row_data["id"] = self.next_id  # <-- Важливо: додай id у словник
        self.next_id += 1

        row = Row(row_data)
        self.rows.append(row)
        return row
        
    def _check_value_type(self, column: Column, value: Any) -> bool:
        """
        Checks if the value is of the correct type for the column.
        """
        if not column.data_type.validate(value):
            raise ValueError(f"Invalid value for column {column.name}: {value}")
        return True

    def get_row(self, index: int) -> Union[Row, None]:
        """
        Retrieves a row by its index.
        """
        return self.rows[index] if 0 <= index < len(self.rows) else None

    def get_by_id(self, row_id: int) -> Union[Row, None]:
        """
        Retrieves a row by its ID.
        """
        return next((row for row in self.rows if row.id == row_id), None)
    
    def get_column(self, column_name: str) -> Column:
        # Get the Column object by name
        for col in self.columns:
            if col.name == column_name:
                return col
        raise ValueError(f"Column {column_name} not found.")

    def update(self, row_id: int, row_data: Dict[str, Any]) -> Union[Row, None]:
        """
        Updates an existing row by its ID.
        """
        row = self.get_by_id(row_id)
        if row is None:
            raise ValueError(f"Row with id {row_id} not found.")

        for column_name, column in self.columns.items():
            value = row_data.get(column_name)
            if value is not None and not column.validate(value):
                raise ValueError(f"Invalid value for column {column_name}: {value}")

        row.data.update(row_data)
        return row

    def delete(self, row_id: int) -> bool:
        """
        Deletes a row by its ID.
        """
        row = self.get_by_id(row_id)
        if row is None:
            return False

        self.rows.remove(row)
        return True

    def __len__(self):
        """
        Returns the number of rows in the table.
        """
        return len(self.rows)

    def __iter__(self):
        """
        Returns an iterator over the rows in the table.
        """
        return iter(self.rows)

    def __repr__(self):
        """
        Returns a string representation of the table.
        """
        return f"Table(name={self.name}, columns={list(self.columns.keys())}, rows={len(self.rows)})"
