from typing import *
from typing import Union, Collection

from dynamoplus.models.query.query import Index
# from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.repository.repositories import IndexDynamoPlusRepository
from dynamoplus.models.query.query import Query, Index
from dynamoplus.repository.models import QueryResult, Model


class IndexService(object):
    index: Index
    collection: Collection
    is_system: bool

    def __init__(self, collection: Collection, index: Index, is_system=False):
        self.index = index
        self.collection = collection
        self.is_system = is_system

    def find_documents(self, document: dict, start_from: str = None, limit: int = None):
        repository = IndexDynamoPlusRepository(self.collection, self.index, self.is_system)
        query = Query(document, self.index, start_from, limit)
        query_result = repository.find(query=query)
        return list(map(lambda r: r.document, query_result.data)), query_result.lastEvaluatedKey
