from typing import *

from dynamoplus.models.query.conditions import Predicate, AnyMatch
from dynamoplus.models.system.index.index import Index
from dynamoplus.repository.models import Query as QueryRepository
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository

from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType
from dynamoplus.service.indexing_decorator import create_document, update_document, delete_document


class DomainService:
    def __init__(self, collection: Collection):
        self.collection = collection

    def get_document(self, id: str):
        data_model = DynamoPlusRepository(self.collection).get(id)
        if data_model:
            return data_model.document

    @create_document
    def create_document(self, document: dict):
        created_data_model = DynamoPlusRepository(self.collection).create(document)
        if created_data_model:
            return created_data_model.document

    @update_document
    def update_document(self, document: dict):
        updated_data_model = DynamoPlusRepository(self.collection).update(document)
        if updated_data_model:
            return updated_data_model.document

    @delete_document
    def delete_document(self, id: str):
        DynamoPlusRepository(self.collection).delete(id)

    def find_all(self, limit: int = None, start_from: str = None):
        return self.query(AnyMatch(), limit, start_from)

    def query(self, predicate: Predicate, index: Index, limit: int = None, start_from: str = None):
        repository = DynamoPlusRepository(self.collection)
        last_evaluated_item = None
        if start_from:
            ## from predicate to index => Index(self.collection, [conditions from predicate])
            last_evaluated_item = repository.get(start_from)
        query: QueryRepository = QueryRepository(predicate, self.collection,index, limit, last_evaluated_item)
        result = repository.query_v2(query)

        return list(map(lambda dm: dm.document, result.data)), result.lastEvaluatedKey
