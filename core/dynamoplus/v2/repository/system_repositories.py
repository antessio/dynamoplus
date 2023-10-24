from __future__ import annotations

import abc
import os
import uuid
from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List

from dynamoplus.v2.repository.repositories_v2 import IndexModel, Model, CounterIncrement

INDEX_FIELD_SEPARATOR = "__"

AGGREGATION_ENTITY_NAME = "aggregation"

AGGREGATION_CONFIGURATION_ENTITY_NAME = "aggregation_configuration"

INDEX_ENTITY_NAME = "index"

INDEX_ORDERING_KEY = "ordering"

COLLECTION_ENTITY_NAME = "collection"

CLIENT_AUTHORIZATION_ENTITY_NAME = "client_authorization"

system_collections = [COLLECTION_ENTITY_NAME, INDEX_ENTITY_NAME, CLIENT_AUTHORIZATION_ENTITY_NAME,
                      AGGREGATION_CONFIGURATION_ENTITY_NAME, AGGREGATION_ENTITY_NAME]


class SystemModel(Model, ABC):
    @classmethod
    def table_name(cls):
        return os.environ['DYNAMODB_SYSTEM_TABLE']


@dataclass(frozen=True)
class ClientAuthorizationEntity(SystemModel):
    uid: uuid.UUID
    payload: dict = None

    @classmethod
    def from_dict(cls, document: dict) -> Model:
        return ClientAuthorizationEntity(document['id'], document)

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return "%s" % CLIENT_AUTHORIZATION_ENTITY_NAME

    def ordering(self):
        return self.object()['ordering'] if 'ordering' in self.object() else None

    def object(self):
        return self.payload


@dataclass(frozen=True)
class ClientAuthorizationByClientId(IndexModel):
    uid: uuid.UUID
    client_id: str
    payload: dict
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp()) * 1000)

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return CLIENT_AUTHORIZATION_ENTITY_NAME

    def index_name(self):
        return "{0}#client_id".format(self.entity_name())

    def index_value(self):
        return "{0}#{1}".format(self.client_id, self.ordering)

    def document(self):
        return self.payload


@dataclass(frozen=True)
class CollectionEntity(SystemModel):
    name: str
    payload: dict = None

    @classmethod
    def from_dict(cls, document: dict) -> Model:
        return CollectionEntity(document['name'], document)

    def id(self):
        return self.name

    def entity_name(self):
        return COLLECTION_ENTITY_NAME

    def ordering(self):
        return self.object()['ordering'] if 'ordering' in self.object() else None

    def object(self):
        return self.payload


@dataclass(frozen=True)
class IndexEntity(SystemModel):
    uid: uuid.UUID
    payload: dict = None

    @classmethod
    def from_dict(cls, document: dict) -> Model:
        return IndexEntity(document['id'], document)

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return "%s" % INDEX_ENTITY_NAME

    def ordering(self):
        return self.payload[INDEX_ORDERING_KEY]

    def object(self):
        return self.payload


@dataclass(frozen=True)
class IndexByCollectionNameEntity(IndexModel):
    uid: uuid.UUID
    collection_name: str
    payload: dict
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp()) * 1000)

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return INDEX_ENTITY_NAME

    def index_name(self):
        return "{0}#collection.name".format(self.entity_name())

    def document(self):
        return self.payload

    def index_value(self):
        return "{0}#{1}".format(self.collection_name, self.ordering)


@dataclass(frozen=True)
class IndexByCollectionNameAndFieldsEntity(IndexModel):
    uid: uuid.UUID
    collection_name: str
    fields: List[str]
    payload: dict
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp()) * 1000)

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return INDEX_ENTITY_NAME

    def index_name(self):
        return "{0}#collection.name__fields".format(self.entity_name())

    def index_value(self):
        return "{0}#{1}#{2}".format(self.collection_name, INDEX_FIELD_SEPARATOR.join(self.fields), self.ordering)

    def document(self):
        return self.payload


@dataclass(frozen=True)
class AggregationConfigurationEntity(SystemModel):
    @classmethod
    def from_dict(cls, document: dict) -> Model:
        return AggregationConfigurationEntity(document['id'], document)

    uid: uuid.UUID
    payload: dict = None

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return "%s" % AGGREGATION_CONFIGURATION_ENTITY_NAME

    def ordering(self):
        return self.payload[INDEX_ORDERING_KEY]

    def object(self):
        return self.payload


@dataclass(frozen=True)
class AggregationConfigurationByCollectionNameEntity(IndexModel):
    uid: uuid.UUID
    collection_name: str
    payload: dict
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp()) * 1000)

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return AGGREGATION_CONFIGURATION_ENTITY_NAME

    def document(self):
        return self.payload

    def index_name(self):
        return "{0}#collection.name".format(self.entity_name())

    def index_value(self):
        return "{0}#{1}".format(self.collection_name, self.ordering)


@dataclass(frozen=True)
class AggregationConfigurationByNameEntity(IndexModel):
    uid: uuid.UUID
    name: str
    payload: dict
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp()) * 1000)

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return AGGREGATION_CONFIGURATION_ENTITY_NAME

    def index_name(self):
        return "{0}#name".format(self.entity_name())

    def document(self):
        return self.payload

    def index_value(self):
        return "{0}#{1}".format(self.name, self.ordering)


@dataclass(frozen=True)
class AggregationEntity(SystemModel):
    uid: uuid.UUID
    payload: dict = None

    @classmethod
    def from_dict(cls, document: dict) -> Model:
        return AggregationEntity(document['id'], document)

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return "%s" % AGGREGATION_ENTITY_NAME

    def ordering(self):
        return self.payload[INDEX_ORDERING_KEY]

    def object(self):
        return self.payload


@dataclass(frozen=True)
class AggregationIncrementCounter(CounterIncrement):

    def entity_name(self) -> str:
        return AGGREGATION_ENTITY_NAME


@dataclass(frozen=True)
class AggregationByAggregationConfigurationNameEntity(IndexModel):

    uid: uuid.UUID
    aggregation_configuration_name: str
    payload: dict
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp()) * 1000)

    def id(self):
        return str(self.uid)

    def document(self):
        return self.payload
    def entity_name(self):
        return AGGREGATION_ENTITY_NAME

    def index_name(self):
        return "{0}#configuration_name".format(self.entity_name())

    def index_value(self):
        return "{0}#{1}".format(self.aggregation_configuration_name, self.ordering)
