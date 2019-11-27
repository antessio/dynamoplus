from typing import *

from dynamoplus.models.system.index.index import Index

class StartFrom:
    def __init__(self, sk: str, data:str):
        self._sk = sk
        self._data = data

    @property
    def sk(self):
        return self._sk

    @sk.setter
    def sk(self, value):
        self._sk = value

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self,value):
        self._data = value

class Query(object):
    def __init__(self, document: dict, index: Index, start_from: StartFrom = None, limit: int = 50):
        self.document = document
        self.index = index
        self.start_from = start_from
        self.limit = limit

    def __eq__(self, o: object) -> bool:
        if isinstance(o,Query):
            return self.document.__eq__(o.document) and self.index.__eq__(o.index)
        return super().__eq__(o)

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self):
        return "index={}, startFrom={}, limit={}, document={}".format(self.index, self.start_from, self.limit,
                                                                      self.document)


