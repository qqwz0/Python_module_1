import unittest
from minidb.table import Table
from minidb.column import Column
from minidb.datatypes import IntegerType, StringType, BooleanType
from minidb.query import SimpleQuery, JoinedTable
from minidb.row import Row

class TestSimpleQuery(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.table = Table("users", [
            Column("id", IntegerType(), nullable=False),
            Column("name", StringType(50), nullable=False),
            Column("age", IntegerType(), nullable=True),
            Column("active", BooleanType(), nullable=False)
        ])
        
        cls.table.insert({"id": 1, "name": "Alice", "age": 30, "active": True})
        cls.table.insert({"id": 2, "name": "Bob", "age": 25, "active": False})
        cls.table.insert({"id": 3, "name": "Charlie", "age": 35, "active": True})

    def test_select_columns(self):
        query = SimpleQuery(self.table).select(["name", "age"]).execute()
        self.assertTrue(all("name" in row.data and "age" in row.data for row in query))
        self.assertTrue(all("id" not in row.data and "active" not in row.data for row in query))

    def test_where_filter(self):
        query = SimpleQuery(self.table).where("age", ">", 25).execute()
        self.assertEqual(len(query), 2)
        self.assertTrue(all(row["age"] > 25 for row in query))

    def test_order_by_ascending(self):
        query = SimpleQuery(self.table).order_by("age").execute()
        ages = [row["age"] for row in query]
        self.assertEqual(ages, sorted(ages))

    def test_order_by_descending(self):
        query = SimpleQuery(self.table).order_by("age", ascending=False).execute()
        ages = [row["age"] for row in query]
        self.assertEqual(ages, sorted(ages, reverse=True))

class TestJoinedTable(unittest.TestCase):

    def setUp(self):
        # Use the DataType instances to define the column types
        column1 = Column("id", IntegerType())  # Use IntegerType() instance, not int
        column2 = Column("name", StringType())  # Use StringType() instance
        self.table1 = Table("table1", [column1, column2])

        column3 = Column("id", IntegerType())  # Same for the second table
        column4 = Column("address", StringType())
        self.table2 = Table("table2", [column3, column4])

        row1 = {"id": 1, "name": "John"}
        row2 = {"id": 2, "name": "Jane"}
        self.table1.insert(row1)
        self.table1.insert(row2)

        row3 = {"id": 1, "address": "New York"}
        row4 = {"id": 2, "address": "Los Angeles"}
        self.table2.insert(row3)
        self.table2.insert(row4)

    def test_inner_join(self):
        joined_table = JoinedTable(self.table1, self.table2, "id")
        joined_rows = list(joined_table)
        self.assertEqual(len(joined_rows), 2)
        self.assertEqual(joined_rows[0].data["name"], "John")
        self.assertEqual(joined_rows[0].data["table2.address"], "New York")
        self.assertEqual(joined_rows[1].data["name"], "Jane")
        self.assertEqual(joined_rows[1].data["table2.address"], "Los Angeles")

    def test_inner_join_no_matches(self):
        # Changing the second table's data to have no matching rows
        self.table2.delete(1)
        self.table2.delete(2)
        joined_table = JoinedTable(self.table1, self.table2, "id")
        joined_rows = list(joined_table)
        self.assertEqual(len(joined_rows), 0)

    def test_repr(self):
        joined_table = JoinedTable(self.table1, self.table2, "id")
        repr_str = repr(joined_table)
        self.assertIn("on=id", repr_str)
        self.assertIn("rows=2", repr_str)

if __name__ == '__main__':
    unittest.main()