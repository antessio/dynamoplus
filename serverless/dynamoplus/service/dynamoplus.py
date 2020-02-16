import os
from typing import *
from dynamoplus.models.system.collection.collection import Collection

from dynamoplus.models.query.query import Query, Index

collectionMetadata = Collection("collection", "name")
indexMetadata = Collection("index", "uid")
systemCollections = {
    "collection": collectionMetadata,
    "index": indexMetadata
}


class DynamoPlusService(object):
    def __init__(self):
        pass

    @staticmethod
    def is_system(collection_name):
        SYSTEM_ENTITIES = os.environ['ENTITIES']
        return collection_name in SYSTEM_ENTITIES.split(",")

    @staticmethod
    def build_index(index_str: str):
        index_str_array = index_str.split("#")
        document_type = index_str_array[0]
        index_name = index_str_array[1]
        parts1 = index_name.split("__ORDER_BY__")
        conditions = parts1[0].split("__")
        return Index(document_type, conditions, ordering_key=parts1[1] if len(parts1) > 1 else None)
