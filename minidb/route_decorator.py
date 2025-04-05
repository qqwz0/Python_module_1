"""
Модуль для декораторів маршрутів та обробки запитів.
Реалізовано просту симуляцію роутінгу для CRUD API.
"""

# Глобальний словник для зберігання маршрутів
routes = {}

def route(path):
    """
    Декоратор для реєстрації функції-обробника за заданим маршрутом.
    """
    def decorator(func):
        routes[path] = func
        return func
    return decorator

def handle_request(path, **kwargs):
    """
    Симулює обробку запиту, викликаючи зареєстровану функцію.
    :param path: Маршрут запиту.
    :param kwargs: Параметри, що передаються у функцію.
    :return: Результат виконання функції або повідомлення про помилку.
    """
    if path in routes:
        return routes[path](**kwargs)
    return {"error": "Route not found"}
