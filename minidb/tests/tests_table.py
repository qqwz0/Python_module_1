import unittest
from minidb.column import Column
from minidb.row import Row
from minidb.table import Table
from minidb.datatypes import IntegerType, StringType  # Assuming you have these data types

class TestTable(unittest.TestCase):
    """Unit tests for the Table class in the minidb module."""

    def setUp(self):
        """Set up the test environment before each test."""
        # Create some columns with a simple data type and nullable set to True
        self.name_column = Column(name='name', data_type=StringType(), nullable=False)
        self.age_column = Column(name='age', data_type=IntegerType(), nullable=True)
        self.columns = [self.name_column, self.age_column]

        # Create a Table instance
        self.table = Table(name='people', columns=self.columns)

    def test_insert_valid_data(self):
        """Test inserting valid data into the table."""
        row_data = {'name': 'Alice', 'age': 30}
        row = self.table.insert(row_data)
        self.assertEqual(len(self.table), 1)  # One row should be added
        self.assertEqual(row.id, 1)  # The first row should have id 1
        self.assertEqual(row['name'], 'Alice')
        self.assertEqual(row['age'], 30)

    def test_insert_invalid_data(self):
        """Test inserting invalid data into the table."""
        row_data = {'name': 'Bob', 'age': 'invalid'}  # Invalid age type
        with self.assertRaises(ValueError):
            self.table.insert(row_data)

    def test_get_row_by_index(self):
        """Test retrieving a row by its index."""
        row_data = {'name': 'Charlie', 'age': 25}
        row = self.table.insert(row_data)
        fetched_row = self.table.get_row(0)
        self.assertEqual(fetched_row, row)  # The row fetched by index should match the inserted row 

    def test_get_row_by_invalid_index(self):
        """Test retrieving a row by an invalid index."""
        row = self.table.get_row(10)  # Invalid index
        self.assertIsNone(row)  # Should return None

    def test_get_by_id(self):
        """Test retrieving a row by its ID."""
        row_data = {'name': 'David', 'age': 40}
        row = self.table.insert(row_data)
        fetched_row = self.table.get_by_id(row.id)
        self.assertEqual(fetched_row, row)  # Should return the same row by ID

    def test_get_by_id_not_found(self):
        """Test retrieving a row by a non-existing ID."""
        fetched_row = self.table.get_by_id(999)  # Non-existing ID
        self.assertIsNone(fetched_row)  # Should return None

    def test_update_row_valid_data(self):
        """Test updating a row with valid data."""
        row_data = {'name': 'Eve', 'age': 50}
        row = self.table.insert(row_data)
        updated_data = {'age': 51}
        updated_row = self.table.update(row.id, updated_data)
        self.assertEqual(updated_row['age'], 51)  # The age should be updated

    def test_update_row_invalid_data(self):
        """Test updating a row with invalid data."""
        row_data = {'name': 'Frank', 'age': 60}
        row = self.table.insert(row_data)
        updated_data = {'age': 'invalid'}  # Invalid age type
        with self.assertRaises(ValueError):
            self.table.update(row.id, updated_data)

    def test_delete_row(self):
        """Test deleting a row from the table."""
        row_data = {'name': 'Grace', 'age': 70}
        row = self.table.insert(row_data)
        result = self.table.delete(row.id)
        self.assertTrue(result)  # Deletion should be successful
        self.assertEqual(len(self.table), 0)  # The table should be empty after deletion

    def test_delete_row_not_found(self):
        """Test deleting a row with a non-existing ID."""
        result = self.table.delete(999)  # Non-existing row ID
        self.assertFalse(result)  # Deletion should fail

    def test_iterate_rows(self):
        """Test iterating over rows in the table."""
        row_data1 = {'name': 'Hannah', 'age': 30}
        row_data2 = {'name': 'Irene', 'age': 35}
        self.table.insert(row_data1)
        self.table.insert(row_data2)

        rows = list(self.table)  # Collect rows in a list
        self.assertEqual(len(rows), 2)  # There should be 2 rows in the table
        self.assertEqual(rows[0]['name'], 'Hannah')
        self.assertEqual(rows[1]['name'], 'Irene')

    def test_repr(self):
        """Test the string representation of the table."""
        row_data = {'name': 'John', 'age': 45}
        self.table.insert(row_data)
        expected_repr = f"Table(name=people, columns={['name', 'age']}, rows=1)"
        self.assertEqual(repr(self.table), expected_repr)  # Check the string representation

    def test_len_empty_table(self):
        """Test the length of an empty table."""
        self.assertEqual(len(self.table), 0)  # The table should be empty initially

    def test_len_after_insert(self):
        """Test the length of the table after inserting a row."""
        row_data = {'name': 'Alice', 'age': 30}
        self.table.insert(row_data)
        self.assertEqual(len(self.table), 1)  # One row should be inserted
    

if __name__ == '__main__':
    unittest.main()
