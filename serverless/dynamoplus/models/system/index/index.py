from typing import *


class Index(object):
    def __init__(self, collection_name: str, conditions: List[str], ordering_key: str = None):
        self._collection_name = collection_name
        self._conditions = conditions
        self._ordering_key = ordering_key

    @property
    def conditions(self):
        return self._conditions

    @property
    def collection_name(self):
        return self._collection_name

    @property
    def index_name(self):
        return "__".join(self._conditions) + (
            "__ORDER_BY__" + self._ordering_key if self._ordering_key is not None else "")

    @collection_name.setter
    def collection_name(self, value):
        self._collection_name = value

    @conditions.setter
    def conditions(self, value):
        self._conditions = value

    def __str__(self):
        return "index={} collectionName={} conditions={} orderingKey={}".format(self.index_name, self._collection_name,
                                                                                self.conditions, self._ordering_key)
