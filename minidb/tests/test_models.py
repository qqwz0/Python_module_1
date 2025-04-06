import unittest

# Dummy in-memory database to simulate minidb behavior
class DummyDB:
    def __init__(self):
        self.storage = {}
        self.next_id = 1

    def create(self, model_name, data):
        data = data.copy()
        data["id"] = self.next_id
        self.storage[self.next_id] = data
        self.next_id += 1
        return data

    def get(self, model_name, obj_id):
        return self.storage.get(obj_id, None)

    def update(self, model_name, obj_id, data):
        if obj_id in self.storage:
            self.storage[obj_id].update(data)
            return self.storage[obj_id]
        return None

    def delete(self, model_name, obj_id):
        if obj_id in self.storage:
            del self.storage[obj_id]
            return True
        return False

# Patching DatabaseRegistry for testing
from minidb.database_registry import DatabaseRegistry
from models.base_model import BaseModel, User, Product, BaseModelMeta

class TestBaseModel(unittest.TestCase):
    def setUp(self):
        self.dummy_db = DummyDB()
        DatabaseRegistry._databases["test"] = self.dummy_db
        User.use_db("test")
        Product.use_db("test")

    def test_model_registry(self):
        self.assertIn("User", BaseModelMeta.registry)
        self.assertIn("Product", BaseModelMeta.registry)

    def test_create(self):
        user = User.create(name="Test", email="test@example.com")
        self.assertIsInstance(user, User)
        self.assertEqual(user.name, "Test")
        self.assertEqual(user.email, "test@example.com")
        self.assertTrue(hasattr(user, "id"))

    def test_get_existing(self):
        created = User.create(name="Test", email="test@example.com")
        fetched = User.get(created.id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.name, "Test")

    def test_get_not_existing(self):
        self.assertIsNone(User.get(999))

    def test_update_existing(self):
        user = User.create(name="Old", email="old@example.com")
        updated = User.update(user.id, name="New")
        self.assertEqual(updated.name, "New")
        self.assertEqual(updated.email, "old@example.com")

    def test_update_not_existing(self):
        self.assertIsNone(User.update(999, name="Ghost"))

    def test_delete_existing(self):
        user = User.create(name="Del", email="del@example.com")
        success = User.delete(user.id)
        self.assertTrue(success)
        self.assertIsNone(User.get(user.id))

    def test_delete_not_existing(self):
        self.assertFalse(User.delete(999))

    def test_to_dict(self):
        user = User.create(name="DictTest", email="dict@example.com")
        user_dict = user.to_dict()
        self.assertEqual(user_dict["name"], "DictTest")
        self.assertEqual(user_dict["email"], "dict@example.com")
        self.assertIn("id", user_dict)

unittest_suite = unittest.TestLoader().loadTestsFromTestCase(TestBaseModel)

if __name__ == "__main__":
    unittest.main()