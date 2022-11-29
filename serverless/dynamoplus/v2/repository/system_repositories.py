from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import List

from aws.dynamodb.dynamodbdao import DynamoDBModel
from dynamoplus.v2.repository.repositories import IndexModel, convert_model_to_dynamo_db_item, Model

INDEX_FIELD_SEPARATOR = "__"

AGGREGATION_ENTITY_NAME = "aggregation"

AGGREGATION_CONFIGURATION_ENTITY_NAME = "aggregation_configuration"

INDEX_ENTITY_NAME = "index"

COLLECTION_ENTITY_NAME = "collection"

CLIENT_AUTHORIZATION_ENTITY_NAME = "client_authorization"

system_collections = [COLLECTION_ENTITY_NAME, INDEX_ENTITY_NAME, CLIENT_AUTHORIZATION_ENTITY_NAME,
                      AGGREGATION_CONFIGURATION_ENTITY_NAME, AGGREGATION_ENTITY_NAME]


# collection_metadata = Collection("collection", "name")
# index_metadata = Collection("index", "name")
# client_authorization_metadata = Collection("client_authorization", "client_id")
# aggregation_configuration_metadata = Collection("aggregation_configuration", "name")
# aggregation_metadata = Collection("aggregation", "name")
# index_by_collection_and_name_metadata = Index(index_metadata.name, ["collection.name", "name"], None)
# index_by_collection_metadata = Index(index_metadata.name, ["collection.name"], None)
# index_by_name_metadata = Index(index_metadata.name, ["name"], None)
# aggregation_configuration_index_by_collection_name = Index("aggregation_configuration", ["collection.name"])
# aggregation_index_by_aggregation_name = Index("aggregation", ["configuration_name"],IndexConfiguration.OPTIMIZE_WRITE)


@dataclass(frozen=True)
class ClientAuthorizationEntity(Model):
    client_id: str
    payload: dict

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> ClientAuthorizationEntity:
        return ClientAuthorizationEntity(str.replace(dynamo_db_model.pk, CLIENT_AUTHORIZATION_ENTITY_NAME + '#', ''),
                                         dynamo_db_model.document)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        return convert_model_to_dynamo_db_item(self)

    def id(self):
        return self.client_id

    def entity_name(self):
        return "%s" % CLIENT_AUTHORIZATION_ENTITY_NAME

    def ordering(self):
        return None

    def object(self):
        return self.payload


@dataclass(frozen=True)
class CollectionEntity(Model):
    name: str
    payload: dict

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> CollectionEntity:
        return CollectionEntity(str.replace(dynamo_db_model.pk, COLLECTION_ENTITY_NAME + '#', ''),
                                dynamo_db_model.document)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        return convert_model_to_dynamo_db_item(self)

    def id(self):
        return self.name

    def entity_name(self):
        return "%s" % COLLECTION_ENTITY_NAME

    def ordering(self):
        return None

    def object(self):
        return self.payload


@dataclass(frozen=True)
class IndexEntity(Model):
    uid: uuid.UUID
    payload: dict

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> IndexEntity:
        return IndexEntity(uuid.UUID(str.replace(dynamo_db_model.pk, INDEX_ENTITY_NAME + '#', '')),
                           dynamo_db_model.document)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        return convert_model_to_dynamo_db_item(self)

    def id(self):
        return str(self.uid)

    def entity_name(self):
        return "%s" % INDEX_ENTITY_NAME

    def ordering(self):
        return None

    def object(self):
        return self.payload


@dataclass(frozen=True)
class IndexByCollectionNameEntity(IndexModel):
    uid: uuid.UUID
    collection_name: str
    payload: dict
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp())*1000)

    def id(self):
        return str(self.uid)

    @property
    def entity_name(self):
        return INDEX_ENTITY_NAME

    def index_name(self):
        return "collection.name"

    def index_value(self):
        return "{0}#{1}".format(self.collection_name, self.ordering)

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> IndexModel:
        data_split = dynamo_db_model.data.split("#")
        collection_name = data_split[0]
        _id = uuid.UUID(str.replace(dynamo_db_model.pk, INDEX_ENTITY_NAME + '#', ''))
        ordering = data_split[1]
        payload = dynamo_db_model.document
        return IndexByCollectionNameEntity(_id,
                                           collection_name,
                                           payload,
                                           ordering)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        pk = INDEX_ENTITY_NAME + "#" + str(self.uid)
        sk = "{0}#{1}".format(INDEX_ENTITY_NAME, self.index_name())
        data = self.index_value()
        document = self.payload
        return DynamoDBModel(pk, sk, data, document)


@dataclass(frozen=True)
class IndexByCollectionNameAndFieldsEntity(IndexModel):
    uid: uuid.UUID
    collection_name: str
    fields: List[str]
    payload: dict
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp())*1000)

    def id(self):
        return self.uid

    @property
    def entity_name(self):
        return INDEX_ENTITY_NAME

    def index_name(self):
        return "collection.name#fields"

    def index_value(self):
        return "{0}#{1}#{2}".format(self.collection_name, INDEX_FIELD_SEPARATOR.join(self.fields), self.ordering)

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> IndexModel:
        data_split = dynamo_db_model.data.split("#")
        collection_name = data_split[0]
        index_name = str.replace(dynamo_db_model.pk, INDEX_ENTITY_NAME + '#', '')
        fields = data_split[1].split(INDEX_FIELD_SEPARATOR)
        ordering = data_split[2]
        payload = dynamo_db_model.document
        return IndexByCollectionNameAndFieldsEntity(index_name,
                                                    collection_name,
                                                    fields,
                                                    payload,
                                                    ordering)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        pk = INDEX_ENTITY_NAME + "#" + self.uid
        sk = "{0}#{1}".format(INDEX_ENTITY_NAME, self.index_name())
        data = self.index_value()
        document = self.payload
        return DynamoDBModel(pk, sk, data, document)