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
        return "__".join(self._conditions) + "__ORDER_BY__" + self._ordering_key

    @conditions.setter
    def conditions(self, value):
        self._conditions = value

    @collection_name.setter
    def collection_name(self, value):
        self._collection_name = value

    @property
    def ordering_key(self):
        return self._ordering_key

    @ordering_key.setter
    def ordering_key(self,value):
        self._ordering_key = value

    def __str__(self):
        return "documentType={} conditions={} orderingKey={}".format(self._collection_name, self._conditions,
                                                                     self._ordering_key)


class Query(object):
    def __init__(self, document: dict, index: Index, start_from: str = None, limit: int = 50):
        self.document = document
        self.index = index
        self.start_from = start_from
        self.limit = limit

    def __eq__(self, o: object) -> bool:
        if isinstance(o,Query):
            return self.document.__eq__(o.document)
        return super().__eq__(o)

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self):
        return "index={}, startFrom={}, limit={}, document={}".format(self.index, self.start_from, self.limit,
                                                                      self.document)
