import uuid
from typing import *

from dynamoplus.utils.utils import auto_str


@auto_str
class Index(object):
    def __init__(self, uid:str, collection_name: str, conditions: List[str], ordering_key: str = None):
        self._collection_name = collection_name
        self._conditions = conditions
        conditions_set = set(self._conditions)
        condition_set_length = len(conditions_set)
        self._range_condition = None
        if condition_set_length != len(self._conditions) and condition_set_length == 1:
            self._range_condition = conditions_set.pop()
        self._ordering_key = ordering_key
        self._index_name = Index.index_name_generator(self._conditions,self._ordering_key)
        self._uid = uid

    @property
    def range_condition(self):
        return self._range_condition

    @range_condition.setter
    def range_condition(self,value):
        self.range_condition = value

    @staticmethod
    def index_name_generator(conditions:List[str], ordering_key:str=None):
        return "__".join(conditions) + ("__ORDER_BY__" + ordering_key if ordering_key is not None else "")

    @property
    def uid(self):
        return self._uid

    @uid.setter
    def uid(self,value):
        self._uid = value

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
        return self._index_name



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
            if o.uid and self._uid:
                return o.uid.__eq__(self._uid)
            else:
                return self._collection_name.__eq__(o.collection_name) \
                       and self._conditions.__eq__(o.conditions) \
                       and self._ordering_key.__eq__(o.ordering_key)
        return super().__eq__(o)
