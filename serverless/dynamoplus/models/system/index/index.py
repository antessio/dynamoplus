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

    @collection_name.setter
    def collection_name(self, value):
        self._collection_name = value

    @property
    def index_name(self):
        return "__".join(self._conditions) + (
            "__ORDER_BY__" + self._ordering_key if self._ordering_key is not None else "")



    @conditions.setter
    def conditions(self, value):
        self._conditions = value

    @property
    def ordering_key(self):
        return self._ordering_key

    @ordering_key.setter
    def ordering_key(self,value):
        self._ordering_key = value

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Index):
            return self._collection_name.__eq__(o.collection_name) and self._conditions.__eq__(
                o.conditions) and self._ordering_key.__eq__(o.ordering_key)
        return super().__eq__(o)

    def __str__(self):
        return "collection_name={} conditions={} ordering_key={}".format(self._collection_name, self._conditions,
                                                                         self._ordering_key)
