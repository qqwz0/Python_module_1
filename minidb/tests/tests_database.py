import unittest
from unittest.mock import patch, MagicMock
import json
from minidb.database import Database
from minidb.table import Table
from minidb.column import Column
from minidb.datatypes import IntegerType, StringType

class TestDatabase(unittest.TestCase):

    def setUp(self):
        """Set up a sample database for testing"""
        self.db = Database("TestDB")
        columns = [
            Column("id", IntegerType()),
            Column("name", StringType())
        ]
        self.table = self.db.create_table("users", columns)

    def test_create_table(self):
        """Test table creation"""
        columns = [
            Column("id", IntegerType()),
            Column("name", StringType())
        ]
        table = self.db.create_table("customers", columns)
        self.assertIn("customers", self.db.tables)
        self.assertEqual(len(table.columns), 2)

    def test_create_table_exists(self):
        """Test creating a table that already exists"""
        columns = [
            Column("id", IntegerType()),
            Column("name", StringType())
        ]
        with self.assertRaises(ValueError):
            self.db.create_table("users", columns)

    def test_drop_table(self):
        """Test dropping a table"""
        self.db.drop_table("users")
        self.assertNotIn("users", self.db.tables)

    def test_drop_table_not_exists(self):
        """Test dropping a non-existent table"""
        with self.assertRaises(ValueError):
            self.db.drop_table("nonexistent_table")

    @patch("builtins.open", new_callable=MagicMock)
    @patch("json.dump")
    def test_save(self, mock_json_dump, mock_open):
        """Test saving the database to a JSON file"""
        self.db.save("test_db.json")
        mock_open.assert_called_once_with("test_db.json", "w", encoding="utf-8")
        mock_json_dump.assert_called_once()

    @patch("builtins.open", new_callable=MagicMock)
    @patch("json.load")
    def test_from_json(self, mock_json_load, mock_open):
        """Test loading a database from a JSON file"""
        mock_json_load.return_value = {
            "name": "TestDB",
            "tables": {
                "users": [{"id": 1, "name": "Alice"}]
            },
            "columns": {
                "users": [
                    {"name": "id", "type": "IntegerType"},
                    {"name": "name", "type": "StringType"}
                ]
            }
        }
        db = Database.from_json("test_db.json")
        self.assertEqual(db.name, "TestDB")
        self.assertIn("users", db.tables)
        self.assertEqual(len(db.tables["users"].columns), 2)

    def test_transaction_commit(self):
        """Test committing a transaction"""
        with self.db as db:
            table = db.tables["users"]
            table.insert({"id": 1, "name": "Alice"})
            self.db.commit()
        self.assertEqual(len(self.db.tables["users"].rows), 1)

    def test_transaction_rollback(self):
        """Test rolling back a transaction"""
        with self.db as db:
            table = db.tables["users"]
            table.insert({"id": 1, "name": "Alice"})
            db.rollback()
        self.assertEqual(len(self.db.tables["users"].rows), 0)

    def test_transaction_in_progress(self):
        """Test that an error is raised if a transaction is already in progress"""
        with self.db as db:
            with self.assertRaises(ValueError):
                with self.db as db2:  # Trying to start another transaction
                    pass

if __name__ == "__main__":
    unittest.main()
