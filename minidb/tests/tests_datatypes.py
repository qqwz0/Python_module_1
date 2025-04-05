from minidb.datatypes import IntegerType, StringType, BooleanType, DateType
import unittest
from datetime import datetime

class TestDataTypes(unittest.TestCase):
    """
    Unit tests for custom data types in the minidb.datatypes module.
    """

    def test_integer_type(self):
        """
        Test validation for IntegerType.
        """
        int_type = IntegerType()
        # Valid integer
        self.assertTrue(int_type.validate(10))
        # Invalid: string instead of integer
        self.assertFalse(int_type.validate("10"))
        # Invalid: float instead of integer
        self.assertFalse(int_type.validate(10.5))

    def test_string_type(self):
        """
        Test validation for StringType with a maximum length.
        """
        str_type = StringType(max_length=5)
        # Valid string within max length
        self.assertTrue(str_type.validate("hello"))
        # Invalid: string exceeds max length
        self.assertFalse(str_type.validate("hello world"))
        # Invalid: integer instead of string
        self.assertFalse(str_type.validate(10))

    def test_boolean_type(self):
        """
        Test validation for BooleanType.
        """
        bool_type = BooleanType()
        # Valid boolean values
        self.assertTrue(bool_type.validate(True))
        self.assertTrue(bool_type.validate(False))
        # Invalid: integer instead of boolean
        self.assertFalse(bool_type.validate(1))
        # Invalid: string instead of boolean
        self.assertFalse(bool_type.validate("True"))

    def test_date_type(self):
        """
        Test validation for DateType.
        """
        date_type = DateType()
        # Valid date string in ISO format
        self.assertTrue(date_type.validate("2025-03-14"))
        # Valid datetime object
        self.assertTrue(date_type.validate(datetime(2025, 3, 14)))
        # Invalid: date string in non-ISO format
        self.assertFalse(date_type.validate("14-03-2025"))
        # Invalid: non-date string
        self.assertFalse(date_type.validate("invalid-date"))

if __name__ == "__main__":
    unittest.main()
