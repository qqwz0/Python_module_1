"""
Головний модуль для симуляції запитів до CRUD API.
"""

from controllers.user_controller import create_user, get_user, update_user, delete_user
from controllers.database_controller import switch_database
from route_decorator import handle_request, route

from minidb.database import Database
from minidb.database_registry import DatabaseRegistry

default_db = Database("default")
DatabaseRegistry.register("default", default_db)

test_db = Database("test")
DatabaseRegistry.register("test", test_db)

@route("/users/create")
def route_create_user(data):
    return create_user(data)

@route("/users/get")
def route_get_user(user_id):
    return get_user(user_id)

@route("/users/update")
def route_update_user(user_id, data):
    return update_user(user_id, data)

@route("/users/delete")
def route_delete_user(user_id):
    return delete_user(user_id)

@route("/database/switch")
def route_switch_database(model_name, db_name):
    return switch_database(model_name, db_name)

def main():
    from models.base_model import User
    User.use_db("default")

    print("=== Default Database ===")
    print(handle_request("/users/create", data={"name": "Alice", "email": "alice@example.com"}))
    print(handle_request("/users/create", data={"name": "Bob", "email": "bob@example.com"}))
    print(handle_request("/users/get", user_id=1))

    print("\n=== Switching to test database ===")
    print(handle_request("/database/switch", model_name="User", db_name="test"))

    print("\n=== Creating user in test database ===")
    print(handle_request("/users/create", data={"name": "Charlie", "email": "charlie@example.com"}))

    print("\n=== Getting users from different databases ===")
    # Переключаємо модель User на default і test бази для перевірки
    from models.base_model import User
    User.use_db("default")
    print("Default DB:", handle_request("/users/get", user_id=1))  # Має бути Alice
    User.use_db("test")
    print("Test DB:", handle_request("/users/get", user_id=1))     # Має бути Charlie

    print("\n=== Updating user ===")
    print(handle_request("/users/update", user_id=1, data={"name": "Alice Updated"}))

    print("\n=== Deleting user ===")
    print(handle_request("/users/delete", user_id=2))

if __name__ == "__main__":
    main()
