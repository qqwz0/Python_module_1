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
        row_data = row_data.copy()  # Avoid modifying the input dictionary

        # Handle 'id' field
        provided_id = row_data.get('id')
        if provided_id is not None:
            if not isinstance(provided_id, int):
                raise ValueError("ID must be an integer.")
            if self.get_by_id(provided_id) is not None:
                raise ValueError(f"Row with id {provided_id} already exists.")
            # Update next_id if the provided ID is higher or equal
            if provided_id >= self.next_id:
                self.next_id = provided_id + 1
        else:
            # Auto-generate ID if not provided
            row_data['id'] = self.next_id
            self.next_id += 1

        # Validate data against column types
        for column_name, column in self.columns.items():
            value = row_data.get(column_name)
            if value is not None and not column.data_type.validate(value):
                raise ValueError(f"Invalid value for column {column_name}: {value}")

        # Create and store the row
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
        for row in self.rows:
            if row.data.get("id") == row_id:
                return row
        return None
    
    def get_column(self, column_name: str) -> Column:
        # Get the Column object by name
        for col in self.columns:
            if col.name == column_name:
                return col
        raise ValueError(f"Column {column_name} not found.")

    def update(self, row_id: int, row_data: Dict[str, Any]) -> Union[Row, None]:
        row = self.get_by_id(row_id)
        if row is None:
            raise ValueError(f"Row with id {row_id} not found.")

        row_data = row_data.copy()
        if 'id' in row_data:
            del row_data['id']

        for column_name, column in self.columns.items():
            value = row_data.get(column_name)
            if value is not None and not column.data_type.validate(value):
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
