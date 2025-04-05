from typing import Any
from datetime import datetime

# Base DataType class
class DataType:
    """
    A base class for data types in the mini database.
    """
    def validate(self, value: Any) -> bool:
        raise NotImplementedError

    def __str__(self):
        return self.__class__.__name__

    def __repr__(self):
        return f"{self.__class__.__name__}()"

class IntegerType(DataType):
    """
    A class used to represent an IntegerType which is a subclass of DataType.
    """
    def validate(self, value: Any) -> bool:
        return isinstance(value, int)

class StringType(DataType):
    """
    A class used to represent a StringType which is a subclass of DataType.
    """
    def __init__(self, max_length: int = 255):
        self.max_length = max_length

    def validate(self, value: Any) -> bool:
        return isinstance(value, str) and len(value) <= self.max_length

class BooleanType(DataType):
    """
    A class used to represent a BooleanType which is a subclass of DataType.
    """
    def validate(self, value: Any) -> bool:
        return isinstance(value, bool)

class DateType(DataType):
    """
    A class used to represent a DateType which is a subclass of DataType.
    """
    def validate(self, value: Any) -> bool:
        if isinstance(value, str):
            try:
                datetime.strptime(value, "%Y-%m-%d")
                return True
            except ValueError:
                return False
        return isinstance(value, datetime)
