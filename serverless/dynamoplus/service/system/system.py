import logging
from typing import *

from dynamoplus.models.query.query import Query
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.repository.models import Model, QueryResult
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType
from dynamoplus.models.system.index.index import Index

collectionMetadata = Collection("collection", "name")
indexMetadata = Collection("index", "uid")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def from_collection_to_dict(collection_metadata: Collection):
    result = {"name": collection_metadata.name, "id_key": collection_metadata.id_key}
    if collection_metadata.ordering_key:
        result["ordering_key"] = collection_metadata.ordering_key
    return result


def from_index_to_dict(index_metadata: Index):
    return {"name": index_metadata.index_name, "collection": {"name": index_metadata.collection_name},
            "conditions": index_metadata.conditions}


def from_dict_to_index(d: dict):
    return Index(d["uid"], d["collection"]["name"], d["conditions"], d["ordering_key"] if "ordering_key" in d else None,
                 d["index_name"] if "index_name" in d else None)


def from_index_to_dict(index_metadata: Index):
    return {
        "uid": index_metadata.uid,
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
        "id_key": collection.id_key,
        "ordering": collection.ordering_key
        ## TODO attributes definition
    }
    # if collection.attributeDefinition:

    return d


def from_dict_to_collection(d: dict):
    return Collection(d["name"], d["id_key"], d["ordering"] if "ordering" in d else None)


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
    def get_all_collections(self):
        index_metadata=Index(None, "collection", [])
        query = Query({}, index_metadata)
        result = IndexDynamoPlusRepository(collectionMetadata, True, index_metadata).find(query)
        if result:
            return list(map(lambda m: from_dict_to_collection(m.document), result.data))
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
            created_index = from_dict_to_index(model.document)
            logger.info("index created {}".format(created_index.__str__()))
            index_by_collection_name = IndexDynamoPlusRepository(indexMetadata,Index(None,"index",["collection.name"]),True).create(model.document)
            logger.info("{} has been indexed {}".format(created_index.collection_name,index_by_collection_name.document))
            index_by_name = IndexDynamoPlusRepository(indexMetadata,Index(None,"index",["collection.name","name"]),True).create(model.document)
            logger.info("{} has been indexed {}".format(created_index.collection_name, index_by_name.document))
            return created_index

    @staticmethod
    def get_index(name: str, collection_name:str):
        # model = DynamoPlusRepository(indexMetadata, True).get(name)
        # if model:
        #     return from_dict_to_index(model.document)
        index = Index(None, "index", ["collection.name","name"])
        query = Query({"name": name,"collection":{"name":collection_name}}, index)
        result: QueryResult = IndexDynamoPlusRepository(indexMetadata, index, True).find(query)
        indexes = list(map(lambda m: from_dict_to_index(m.document), result.data))
        if len(indexes) == 0:
            return None
        else:
            return indexes[0]

    @staticmethod
    def delete_index(name: str):
        DynamoPlusRepository(indexMetadata, True).delete(name)

    @staticmethod
    def find_indexes_from_collection_name(collection_name: str):
        index = Index(None, "index", ["collection.name"])
        query = Query({"collection": {"name": collection_name}}, index)
        result: QueryResult = IndexDynamoPlusRepository(indexMetadata, index, True).find(query)
        return list(map(lambda m: from_dict_to_index(m.document), result.data))

    @staticmethod
    def find_collection_by_example(example: Collection):
        index = Index(None, "collection", ["name"])
        query = Query({"name": example.name}, index)
        result: QueryResult = IndexDynamoPlusRepository(indexMetadata, index, True).find(query)
        return list(map(lambda m: from_dict_to_collection(m.document), result.data))
