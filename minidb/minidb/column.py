from typing import Any
from .datatypes import DataType

class Column:
    """
    Represents a column in a database table.
    Attributes:
        name (str): The name of the column.
        data_type (DataType): The data type of the column.
        nullable (bool): Indicates whether the column can contain null values. Defaults to True.
    Methods:
        validate(value: Any) -> bool:
            Validates if the given value conforms to the column's data type and nullability.
        __repr__() -> str:
            Returns a string representation of the Column instance.
        count(column_name: str) -> int:
            Counts the number of non-null values in the specified column.
        sum(column_name: str) -> float:
            Calculates the sum of the non-null values in the specified column.
        avg(column_name: str) -> float:
            Calculates the average of the non-null values in the specified column.
    """
    def __init__(self, name: str, data_type: DataType, nullable: bool = True):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable

    def validate(self, value: Any) -> bool:
        if value is None:
            return self.nullable  # Reject None if not nullable
        return self.data_type.validate(value)
    
    def count(self, column_name: str) -> int:
        # Check if the column_name exists in the row
        if column_name not in self.rows[0].data:
            raise KeyError(f"Column {column_name} does not exist.")
        return sum(1 for row in self.rows if row[column_name] is not None)

    def sum(self, column_name: str) -> float:
        # Check if the column_name exists in the row
        if column_name not in self.rows[0].data:
            raise KeyError(f"Column {column_name} does not exist.")
        return sum(row[column_name] for row in self.rows if row[column_name] is not None)

    def avg(self, column_name: str) -> float:
        # Check if the column_name exists in the row
        if column_name not in self.rows[0].data:
            raise KeyError(f"Column {column_name} does not exist.")
        values = [row[column_name] for row in self.rows if row[column_name] is not None]
        return sum(values) / len(values) if values else 0
    
    def __repr__(self):
        return f"Column(name={self.name}, data_type={repr(self.data_type)}, nullable={self.nullable})"
