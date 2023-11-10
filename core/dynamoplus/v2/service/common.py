from dynamoplus.models.system.collection.collection import Collection


def is_system(collection: Collection) -> bool:
    return collection.name in ["collection", "index", "client_authorization", "aggregation_configuration",
                               "aggregation"]


def is_system_collection(collection_name: str) -> bool:
    return collection_name in ["collection", "index", "client_authorization", "aggregation_configuration",
                               "aggregation"]


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]
