"""
Контролер для роботи з користувачами.
"""

from models.base_model import User

def create_user(data):
    """
    Створює нового користувача.
    :param data: Словник з даними користувача.
    :return: Словникове представлення створеного користувача.
    """
    user = User.create(**data)
    return user.to_dict()

def get_user(user_id):
    """
    Отримує дані користувача за ID.
    :param user_id: ID користувача.
    :return: Словникове представлення користувача або повідомлення про помилку.
    """
    user = User.get(user_id)
    if user:
        return user.to_dict()
    return {"error": "User not found"}

def update_user(user_id, data):
    """
    Оновлює дані користувача за ID.
    :param user_id: ID користувача.
    :param data: Словник з новими даними.
    :return: Словникове представлення оновленого користувача або повідомлення про помилку.
    """
    user = User.update(user_id, **data)
    if user:
        return user.to_dict()
    return {"error": "User not found or update failed"}

def delete_user(user_id):
    """
    Видаляє користувача за ID.
    :param user_id: ID користувача.
    :return: Словник з інформацією про успішність видалення.
    """
    success = User.delete(user_id)
    return {"deleted": success}
