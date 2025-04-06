# sqlite_database.py
"""
Модуль для роботи з SQLite базою даних.
"""

import sqlite3
from sqlite3 import Error

class SQLiteDatabase:
    """
    Клас для роботи з SQLite базою даних.
    """
    def __init__(self, db_file: str):
        """
        Ініціалізує з’єднання з SQLite базою даних.
        :param db_file: Файл бази даних.
        """
        self.db_file = db_file
        self.conn = None
        self.connect()

    def connect(self):
        """Встановлює з’єднання з базою даних."""
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.conn.row_factory = sqlite3.Row  # для отримання результатів у вигляді словників
        except Error as e:
            print(f"Помилка під час з’єднання: {e}")

    def execute(self, query: str, params: tuple = ()):
        """
        Виконує SQL запит з параметрами.
        :param query: SQL запит.
        :param params: Кортеж параметрів.
        :return: Курсор.
        """
        try:
            cur = self.conn.cursor()
            cur.execute(query, params)
            self.conn.commit()
            return cur
        except Error as e:
            print(f"Помилка виконання запиту: {e}")
            self.conn.rollback()
            return None

    def create_table(self, table_name: str, columns: dict):
        """
        Створює таблицю, якщо її немає.
        :param table_name: Назва таблиці.
        :param columns: Словник, де ключі — назви стовпців, а значення — типи даних (наприклад, "INTEGER", "TEXT").
        """
        columns_def = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, {columns_def})"
        self.execute(query)

    def create(self, table_name: str, data: dict):
        """
        Вставляє новий запис у таблицю.
        :param table_name: Назва таблиці.
        :param data: Словник з даними.
        :return: Словник з даними запису включно з id.
        """
        keys = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        values = tuple(data.values())
        query = f"INSERT INTO {table_name} ({keys}) VALUES ({placeholders})"
        cur = self.execute(query, values)
        if cur:
            data['id'] = cur.lastrowid
            return data
        return None

    def get(self, table_name: str, obj_id: int):
        """
        Отримує запис за ID.
        :param table_name: Назва таблиці.
        :param obj_id: ID запису.
        :return: Словник з даними запису або None.
        """
        query = f"SELECT * FROM {table_name} WHERE id = ?"
        cur = self.execute(query, (obj_id,))
        row = cur.fetchone() if cur else None
        return dict(row) if row else None

    def update(self, table_name: str, obj_id: int, data: dict):
        """
        Оновлює запис за ID.
        :param table_name: Назва таблиці.
        :param obj_id: ID запису.
        :param data: Словник з новими даними.
        :return: Словник з оновленими даними або None.
        """
        set_clause = ", ".join([f"{key} = ?" for key in data.keys()])
        values = tuple(data.values()) + (obj_id,)
        query = f"UPDATE {table_name} SET {set_clause} WHERE id = ?"
        cur = self.execute(query, values)
        if cur and cur.rowcount:
            return self.get(table_name, obj_id)
        return None

    def delete(self, table_name: str, obj_id: int):
        """
        Видаляє запис за ID.
        :param table_name: Назва таблиці.
        :param obj_id: ID запису.
        :return: True, якщо видалення успішне, інакше False.
        """
        query = f"DELETE FROM {table_name} WHERE id = ?"
        cur = self.execute(query, (obj_id,))
        return cur.rowcount > 0 if cur else False

    def close(self):
        """Закриває з’єднання з базою даних."""
        if self.conn:
            self.conn.close()
