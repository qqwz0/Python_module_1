import unittest
from minidb.datatypes import IntegerType, StringType, BooleanType, DateType, DataType
from minidb.column import Column  # Import your Column class
from datetime import datetime
from minidb.row import Row

class TestColumn(unittest.TestCase):
    """
    Unit tests for the Column class in the minidb package.
    """

    def test_integer_column(self):
        """
        Test validation for an integer column.
        """
        col = Column("age", IntegerType(), nullable=False)
        # Valid integer value
        self.assertTrue(col.validate(25))
        # Invalid string value
        self.assertFalse(col.validate("25"))
        # Invalid None value for non-nullable column
        self.assertFalse(col.validate(None))
    
    def test_nullable_column(self):
        """
        Test validation for a nullable integer column.
        """
        col = Column("optional", IntegerType(), nullable=True)
        # None value is valid for nullable column
        self.assertTrue(col.validate(None))
    
    def test_string_column(self):
        """
        Test validation for a string column with a maximum length.
        """
        col = Column("name", StringType(10))
        # Valid string value within length limit
        self.assertTrue(col.validate("Alice"))
        # Invalid string value exceeding length limit
        self.assertFalse(col.validate("This is a very long name"))
        # Invalid non-string value
        self.assertFalse(col.validate(123))
    
    def test_boolean_column(self):
        """
        Test validation for a boolean column.
        """
        col = Column("is_active", BooleanType())
        # Valid boolean values
        self.assertTrue(col.validate(True))
        self.assertTrue(col.validate(False))
        # Invalid integer value
        self.assertFalse(col.validate(1))
        # Invalid string value
        self.assertFalse(col.validate("True"))
    
    def test_date_column(self):
        """
        Test validation for a date column.
        """
        col = Column("created_at", DateType())
        # Valid date string
        self.assertTrue(col.validate("2025-03-14"))
        # Valid datetime object
        self.assertTrue(col.validate(datetime(2025, 3, 14)))
        # Invalid date string format
        self.assertFalse(col.validate("14-03-2025"))
        # Invalid integer value
        self.assertFalse(col.validate(123456))

    def setUp(self):
        # Sample mock data for rows (you would likely use a specific data type class here)
        self.column_name = "price"
        self.column = Column(name=self.column_name, data_type=DataType())  # Replace DataType() with your actual DataType
        
        # Creating mock rows with sample data
        self.rows = [
            Row({"price": 10}),
            Row({"price": 20}),
            Row({"price": 30}),
            Row({"price": None}),  # This row contains a None value
        ]
        
        # Assigning rows to the column for testing (you may need to modify this based on your actual design)
        self.column.rows = self.rows

    def test_count(self):
        # Test count for non-None values
        result = self.column.count("price")
        self.assertEqual(result, 3)  # We have 3 non-None values (10, 20, 30)

    def test_sum(self):
        # Test sum of values
        result = self.column.sum("price")
        self.assertEqual(result, 60)  # 10 + 20 + 30 = 60

    def test_avg(self):
        # Test average of values
        result = self.column.avg("price")
        self.assertEqual(result, 20)  # (10 + 20 + 30) / 3 = 20

    def test_avg_with_no_values(self):
        # Test average when all values are None
        self.column.rows = [Row({"price": None}), Row({"price": None})]
        result = self.column.avg("price")
        self.assertEqual(result, 0)  # No values, should return 0

    def test_invalid_column_name(self):
        # Test behavior when an invalid column name is passed
        with self.assertRaises(KeyError):
            self.column.count("non_existing_column")

if __name__ == "__main__":
    unittest.main()