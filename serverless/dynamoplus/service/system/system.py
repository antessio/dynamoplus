from typing import *

from dynamoplus.models.indexes.indexes import Query
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.repository.models import Model, QueryResult, DataModel
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import Index

collectionMetadata = Collection("collection", "name")
indexMetadata = Collection("index", "name")


def from_dict_to_index(d: dict):
    return Index(d["collection"]["name"], d["conditions"], d["ordering_key"] if "ordering_key" in d else None)


def from_index_to_dict(index_metadata: Index):
    return {
        "name": index_metadata.index_name,
        "collection": {
            "name": index_metadata.collection_name
        },
        "ordering_key": index_metadata._ordering_key,
        "conditions": index_metadata.conditions

    }


def from_collection_to_dict(collection: Collection):
    d = {
        "name": collection.name,
        "idKey": collection.id_key,
        "ordering": collection.ordering_key
        ## TODO attributes definition
    }
    # if collection.attributeDefinition:

    return d


def from_dict_to_collection(d: dict):
    return Collection(d["name"], d["idKey"], d["ordering"] if "ordering" in d else None)


class SystemService:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def create_collection(metadata: Collection):
        collection = from_collection_to_dict(metadata)
        repository = DynamoPlusRepository(collectionMetadata, True)
        model = repository.create(collection)
        return from_dict_to_collection(model.document)

    # def updateCollection(self, metadata:Collection):
    #     collection=self.fromCollectionToDict(metadata)
    #     repository = DynamoPlusRepository(collectionMetadata)
    #     model = repository.update(collection)
    #     return self.fromDictToCollection(model.document)
    @staticmethod
    def delete_collection(name: str):
        DynamoPlusRepository(collectionMetadata, True).delete(name)

    @staticmethod
    def get_collection_by_name(name: str):
        model = DynamoPlusRepository(collectionMetadata, True).get(name)
        if model:
            return from_dict_to_collection(model.document)

    @staticmethod
    def create_index(i: Index) -> Index:
        index = from_index_to_dict(i)
        repository = DynamoPlusRepository(indexMetadata, True)
        model = repository.create(index)
        if model:
            return from_dict_to_index(model.document)

    @staticmethod
    def get_index(name: str):
        model = DynamoPlusRepository(indexMetadata, True).get(name)
        if model:
            return from_dict_to_index(model.document)

    @staticmethod
    def delete_index(name: str):
        DynamoPlusRepository(indexMetadata, True).delete(name)

    # def getIndexFromCollectioName(self, collectionName: str):
    #     pass
    @staticmethod
    def find_collection_by_example(example: Collection, query_id: str):
        index = Index("collection", ["collection.name"])
        query = Query({"name": example.name}, index)
        result: QueryResult = IndexDynamoPlusRepository(indexMetadata, index, True).find(query)
        return list(map(lambda d: from_dict_to_collection(d.data_model.document), result.data))
