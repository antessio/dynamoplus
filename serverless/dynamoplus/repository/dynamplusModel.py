from typing import *
import logging

from dynamoplus.utils.utils import convertToString, find_value, get_values_by_key_recursive
## pynamodb
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, JSONAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
##
import os

logging.basicConfig(level=logging.INFO)


class IndexDataModel(GlobalSecondaryIndex):
    class Meta:
        # index_name is optional, but can be provided to override the default name
        index_name = 'sk-data-index'
        # All attributes are projected
        projection = AllProjection()
        read_capacity_units = 1
        write_capacity_units = 1

    sk = UnicodeAttribute(hash_key=True)
    data = UnicodeAttribute(range_key=True)


class DocumentModel(Model):
    class Meta:
        table_name = os.environ.get('DYNAMODB_DOMAIN_TABLE')
        region = os.environ.get('REGION')

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    data = UnicodeAttribute()
    document = JSONAttribute()
    skDataIndex = IndexDataModel()

    @staticmethod
    def setup_model(model, region, table_name):
        model.Meta.table_name = table_name
        model.Meta.region = region


class Document:
    model: DocumentModel
    document: dict
    collection_name: str
    order_key: str
    id_key: str

    def __init__(self, collection: Collection, document: dict):
        self.id_key = collection.id_key
        self.order_key = collection.ordering_key
        self.collection_name = collection.name
        self.document = document
        self.model = DocumentModel(get_pk(self.document, self.collection_name, self.id_key),
                                   get_sk(self.document, self.collection_name),
                                   data=get_data(self.document, self.id_key, self.order_key), document=self.document)


def get_pk(document: dict, collection_name: str, id_key: str):
    return document["pk"] if "pk" in document else (
        collection_name + "#" + document[id_key] if id_key in document else None)


def get_sk(document: dict, collection_name: str):
    return document["sk"] if "sk" in document else collection_name


def get_data(document: dict, id_key: str, ordering_key: str = None):
    if "data" in document:
        return document["data"]
    else:
        data = convertToString(document[id_key])
        orderValue = get_order_value(document, ordering_key)
        if orderValue:
            data = document[id_key] + "#" + orderValue
        return data


def get_order_value(document: dict, ordering_key: str):
    if ordering_key:
        return find_value(document, ordering_key.split("."))
