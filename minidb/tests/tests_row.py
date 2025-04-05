import unittest
from minidb.row import Row  # Import your Row class

class TestRow(unittest.TestCase):
    """
    Unit tests for the Row class.
    """

    def setUp(self):
        """
        Set up the test environment.
        This will reset the _id_counter before running each test.
        """
        Row._id_counter = 1

    def test_initialization(self):
        """
        Test the initialization of a Row object.
        """
        data = {'name': 'Alice', 'age': 30}
        row = Row(data)
        self.assertEqual(row.id, 1)  # First row should have id 1
        self.assertEqual(row.data, data)  # Data should match the input

    def test_getitem(self):
        """
        Test the __getitem__ method of the Row class.
        """
        data = {'name': 'Bob', 'age': 25}
        row = Row(data)
        self.assertEqual(row['name'], 'Bob')  # Should return 'Bob'
        self.assertEqual(row['age'], 25)  # Should return 25
        self.assertIsNone(row['non_existent_key'])  # Should return None for nonexistent key

    def test_setitem(self):
        """
        Test the __setitem__ method of the Row class.
        """
        data = {'name': 'Charlie'}
        row = Row(data)
        row['name'] = 'David'  # Update existing key
        row['age'] = 28  # Add new key
        self.assertEqual(row['name'], 'David')  # Name should be updated
        self.assertEqual(row['age'], 28)  # Age should be added

    def test_equality(self):
        """
        Test the equality comparison of Row objects.
        """
        data1 = {'name': 'Eve', 'age': 22}
        data2 = {'name': 'Eve', 'age': 22}
        data3 = {'name': 'Frank', 'age': 33}
        
        row1 = Row(data1)
        row2 = Row(data2)
        row3 = Row(data3)
        
        self.assertEqual(row1, row2)  # Should be equal, as data is the same
        self.assertNotEqual(row1, row3)  # Should not be equal, data is different

    def test_repr(self):
        """
        Test the __repr__ method of the Row class.
        """
        data = {'name': 'Grace', 'age': 40}
        row = Row(data)
        self.assertEqual(repr(row), f"Row(id={row.id}, data={data})")  # Check if string representation is correct

    def test_id_counter(self):
        """
        Test the id counter of the Row class.
        """
        data1 = {'name': 'Helen'}
        data2 = {'name': 'Ivan'}
        
        row1 = Row(data1)
        row2 = Row(data2)
        
        self.assertEqual(row1.id, 1)  # First row should have id 1
        self.assertEqual(row2.id, 2)  # Second row should have id 2


if __name__ == '__main__':
    unittest.main()