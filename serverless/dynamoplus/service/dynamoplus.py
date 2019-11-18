from typing import *
from dynamoplus.models.query.query import Index
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.service.indexes import IndexService
from dynamoplus.repository.repositories import DynamoPlusRepository
from dynamoplus.models.query.query import Query, Index

# from dynamoplus.repository.models import QueryResult, Document

collectionMetadata = Collection("collection", "name")
indexMetadata = Collection("index", "name")
systemCollections = {
    "collection": collectionMetadata,
    "index": indexMetadata
}


class DynamoPlusService(object):
    def __init__(self):
        pass


    @staticmethod
    def build_index(index_str: str):
        index_str_array = index_str.split("#")
        document_type = index_str_array[0]
        index_name = index_str_array[1]
        parts1 = index_name.split("__ORDER_BY__")
        conditions = parts1[0].split("__")
        return Index(document_type, conditions, ordering_key=parts1[1] if len(parts1) > 1 else None)
