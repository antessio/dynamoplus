from typing import *

from dynamoplus.models.indexes.indexes import Query
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
        document = DynamoPlusRepository(self.collection).get(id)
        if document:
            return document.document

    def create_document(self, document: dict):
        created_document = DynamoPlusRepository(self.collection).create(document)
        if created_document:
            return created_document.document

    def update_document(self, document: dict):
        updated_document = DynamoPlusRepository(self.collection).update(document)
        if updated_document:
            return updated_document.document

    def delete_document(self, id: str):
        DynamoPlusRepository(self.collection).delete(id)

    def find_all(self):
        index = Index(self.collection.name, [])
        return self.find_by_index(index, {})

    def find_by_index(self, index: Index, example: dict):
        query = Query(example, index)
        result = IndexDynamoPlusRepository(self.collection, index, False).find(query)
        return list(map(lambda data_model: data_model.document, result.data)), result.lastEvaluatedKey
