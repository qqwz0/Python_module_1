"""
Модуль, що містить BaseModel та приклади користувацьких моделей.
"""

from minidb.database_registry import DatabaseRegistry

class BaseModelMeta(type):
    """
    Метаклас, який реєструє всі моделі у внутрішньому реєстрі.
    """
    registry = {}

    def __new__(cls, name, bases, attrs):
        obj = super().__new__(cls, name, bases, attrs)
        if name != "BaseModel":
            BaseModelMeta.registry[name] = obj
        return obj

class BaseModel(metaclass=BaseModelMeta):
    """
    Базова модель з підтримкою вибору бази даних.
    """
    _db_instance = DatabaseRegistry.get("default")  # За замовчуванням працюємо з базою "default"

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def to_dict(self):
        """Повертає словникове представлення моделі."""
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def use_db(cls, db_name):
        """
        Динамічно змінює базу даних для моделі.
        :param db_name: Назва бази даних.
        """
        db = DatabaseRegistry.get(db_name)
        if db:
            cls._db_instance = db
        else:
            raise ValueError(f"Database '{db_name}' not found")

    @classmethod
    def create(cls, **data):
        """
        Створює новий об'єкт у вибраній базі даних.
        :param data: Дані для створення об'єкта.
        :return: Екземпляр моделі.
        """
        created_obj_data = cls._db_instance.create(cls.__name__, data)
        return cls(**created_obj_data)

    @classmethod
    def get(cls, obj_id):
        """
        Отримує об'єкт за його ID.
        :param obj_id: ID об'єкта.
        :return: Екземпляр моделі або None.
        """
        obj_data = cls._db_instance.get(cls.__name__, obj_id)
        if obj_data:
            return cls(**obj_data)
        return None

    @classmethod
    def update(cls, obj_id, **data):
        """
        Оновлює об'єкт за його ID.
        :param obj_id: ID об'єкта.
        :param data: Дані для оновлення.
        :return: Оновлений екземпляр моделі або None.
        """
        updated_data = cls._db_instance.update(cls.__name__, obj_id, data)
        if updated_data:
            return cls(**updated_data)
        return None

    @classmethod
    def delete(cls, obj_id):
        """
        Видаляє об'єкт за його ID.
        :param obj_id: ID об'єкта.
        :return: Булеве значення успішності операції.
        """
        return cls._db_instance.delete(cls.__name__, obj_id)

# Приклад користувацьких моделей
class User(BaseModel):
    """
    Модель користувача.
    Атрибути: id, name, email.
    """
    pass  # Атрибути визначаються динамічно

class Product(BaseModel):
    """
    Модель продукту.
    Атрибути: id, title, price.
    """
    pass  # Атрибути визначаються динамічно
