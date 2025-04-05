from typing import Any, Dict

class Row:
    """
    A class to represent a row in a mini database.
    Attributes:
    -----------
    _id_counter : int
        A class variable to keep track of the next ID to assign.
    id : int
        The unique ID assigned to the row.
    data : Dict[str, Any]
        The data stored in the row.
    Methods:
    --------
    __init__(self, data: Dict[str, Any]):
        Initializes a new Row instance with the given data and assigns a unique ID.
    __getitem__(self, key: str) -> Any:
        Returns the value associated with the given key in the row data.
    __setitem__(self, key: str, value: Any):
        Sets the value for the given key in the row data.
    __eq__(self, other):
        Checks if two Row instances are equal based on their data.
    __repr__(self):
        Returns a string representation of the Row instance.
    """
    _id_counter = 1  # Class variable to keep track of the next ID to assign

    def __init__(self, data: Dict[str, Any]):
        self.id = Row._id_counter  # Assign a unique ID to the row
        Row._id_counter += 1  # Increment the ID counter for the next row
        self.data = data  # Store the row data

    def __getitem__(self, key: str) -> Any:
        return self.data.get(key, None)

    def __setitem__(self, key: str, value: Any):
        self.data[key] = value

    def __eq__(self, other):
        return isinstance(other, Row) and self.data == other.data

    def __repr__(self):
        return f"Row(id={self.id}, data={self.data})"
