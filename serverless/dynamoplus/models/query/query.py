from typing import *

from dynamoplus.models.system.index.index import Index

class Query(object):
    def __init__(self, document: dict, index: Index, limit: int = 50, start_from: str = None):
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


