import unittest
from minidb.database_registry import DatabaseRegistry
from minidb.database import Database

class DummyDatabase:
    def __init__(self, name):
        self.name = name

class TestDatabaseRegistry(unittest.TestCase):

    def setUp(self):
        # Reset the registry before each test
        DatabaseRegistry._databases = {}

    def test_register_and_get(self):
        """Test that a database instance can be registered and then retrieved."""
        db = DummyDatabase("TestDB")
        DatabaseRegistry.register("TestDB", db)
        self.assertIs(DatabaseRegistry.get("TestDB"), db)

    def test_get_nonexistent(self):
        """Test that getting a non-existent database returns None."""
        self.assertIsNone(DatabaseRegistry.get("NonExistentDB"))

    def test_list(self):
        """Test that list returns all registered database names."""
        db1 = DummyDatabase("DB1")
        db2 = DummyDatabase("DB2")
        DatabaseRegistry.register("DB1", db1)
        DatabaseRegistry.register("DB2", db2)
        self.assertCountEqual(DatabaseRegistry.list(), ["DB1", "DB2"])

    def test_register_overwrite(self):
        """Test that registering a database under an existing name overwrites the previous instance."""
        db1 = DummyDatabase("DB")
        db2 = DummyDatabase("DB")
        DatabaseRegistry.register("DB", db1)
        self.assertIs(DatabaseRegistry.get("DB"), db1)
        # Overwrite with a new instance
        DatabaseRegistry.register("DB", db2)
        self.assertIs(DatabaseRegistry.get("DB"), db2)

if __name__ == "__main__":
    unittest.main()
