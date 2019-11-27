from typing import *

from dynamoplus.models.query.query import Query
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
# from dynamoplus.repository.models import Model
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType

from dynamoplus.models.system.index.index import Index


# collectionMetadata = Collection("collection","name")
# indexMetadata = Collection("index","name")
class DomainService:
    def __init__(self, collection: Collection):
        self.collection = collection

    def get_document(self, id: str):
        data_model = DynamoPlusRepository(self.collection).get(id)
        if data_model:
            return data_model.document

    def create_document(self, document: dict):
        created_data_model = DynamoPlusRepository(self.collection).create(document)
        if created_data_model:
            return created_data_model.document

    def update_document(self, document: dict):
        updated_data_model = DynamoPlusRepository(self.collection).update(document)
        if updated_data_model:
            return updated_data_model.document

    def delete_document(self, id: str):
        DynamoPlusRepository(self.collection).delete(id)

    def find_all(self, limit: int = None, start_from: str = None):
        index = Index(None,self.collection.name, [])
        return self.find_by_index(index, {},start_from,limit)

    def find_by_index(self, index: Index, example: dict, start_from:str = None, limit:int = None):
        query = Query(example, index, start_from,limit)
        result = IndexDynamoPlusRepository(self.collection, index, False).find(query)
        return list(map(lambda dm: dm.document, result.data)), result.lastEvaluatedKey
