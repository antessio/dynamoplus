from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Type

from aws.dynamodb.dynamodbdao import DynamoDBModel, DynamoDBKey, Counter
from dynamoplus.v2.repository.repositories_v2 import IndexModel, convert_model_to_dynamo_db_item, Model, Query

INDEX_FIELD_SEPARATOR = "__"

AGGREGATION_ENTITY_NAME = "aggregation"

AGGREGATION_CONFIGURATION_ENTITY_NAME = "aggregation_configuration"

INDEX_ENTITY_NAME = "index"

COLLECTION_ENTITY_NAME = "collection"

CLIENT_AUTHORIZATION_ENTITY_NAME = "client_authorization"

system_collections = [COLLECTION_ENTITY_NAME, INDEX_ENTITY_NAME, CLIENT_AUTHORIZATION_ENTITY_NAME,
                      AGGREGATION_CONFIGURATION_ENTITY_NAME, AGGREGATION_ENTITY_NAME]


@dataclass(frozen=True)
class ClientAuthorizationEntity(Model):
    uid: uuid.UUID
    payload: dict = None

    @classmethod
    def from_dynamo_db_key(cls, last_evaluated_key: DynamoDBKey) -> str:
        return str.replace(last_evaluated_key.partition_key, CLIENT_AUTHORIZATION_ENTITY_NAME + '#', '')

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> ClientAuthorizationEntity:
        return ClientAuthorizationEntity(uuid.UUID(str.replace(dynamo_db_model.pk, CLIENT_AUTHORIZATION_ENTITY_NAME + '#', '')),
                                         dynamo_db_model.document)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        return convert_model_to_dynamo_db_item(self)

    def id(self):
        return str(self.uid)

    @classmethod
    def entity_name(cls):
        return "%s" % CLIENT_AUTHORIZATION_ENTITY_NAME

    def ordering(self):
        return None

    def object(self):
        return self.payload


@dataclass(frozen=True)
class CollectionEntity(Model):
    name: str
    payload: dict = None

    @classmethod
    def from_dynamo_db_key(cls, last_evaluated_key: DynamoDBKey) -> str:
        return str.replace(last_evaluated_key.partition_key, COLLECTION_ENTITY_NAME + '#', '')

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> CollectionEntity:
        return CollectionEntity(str.replace(dynamo_db_model.pk, COLLECTION_ENTITY_NAME + '#', ''),
                                dynamo_db_model.document)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        return convert_model_to_dynamo_db_item(self)

    def id(self):
        return self.name

    @classmethod
    def entity_name(cls):
        return "%s" % COLLECTION_ENTITY_NAME

    def ordering(self):
        return None

    def object(self):
        return self.payload


@dataclass(frozen=True)
class IndexEntity(Model):
    uid: uuid.UUID
    payload: dict = None

    @classmethod
    def from_dynamo_db_key(cls, last_evaluated_key: DynamoDBKey) -> str:
        return str.replace(last_evaluated_key.partition_key, INDEX_ENTITY_NAME + '#', '')

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> IndexEntity:
        return IndexEntity(uuid.UUID(str.replace(dynamo_db_model.pk, INDEX_ENTITY_NAME + '#', '')),
                           dynamo_db_model.document)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        return convert_model_to_dynamo_db_item(self)

    def id(self):
        return str(self.uid)

    @classmethod
    def entity_name(cls):
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
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp()) * 1000)

    def id(self):
        return str(self.uid)

    @classmethod
    def entity_name(cls):
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

    def to_dynamo_db_model(self) -> DynamoDBModel:
        pk = "{0}#{1}".format(INDEX_ENTITY_NAME, str(self.uid))
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
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp()) * 1000)

    def id(self):
        return self.uid

    @classmethod
    def entity_name(cls):
        return INDEX_ENTITY_NAME

    def index_name(self):
        return "collection.name#fields"

    def index_value(self):
        return "{0}#{1}#{2}".format(self.collection_name, INDEX_FIELD_SEPARATOR.join(self.fields), self.ordering)

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> IndexModel:
        data_split = dynamo_db_model.data.split("#")
        collection_name = data_split[0]
        uid = uuid.UUID(str.replace(dynamo_db_model.pk, INDEX_ENTITY_NAME + '#', ''))
        fields = data_split[1].split(INDEX_FIELD_SEPARATOR)
        ordering = data_split[2]
        payload = dynamo_db_model.document
        return IndexByCollectionNameAndFieldsEntity(uid,
                                                    collection_name,
                                                    fields,
                                                    payload,
                                                    ordering)

    def to_dynamo_db_model(self) -> DynamoDBModel:
        pk = "{0}#{1}".format(INDEX_ENTITY_NAME, str(self.uid))
        sk = "{0}#{1}".format(INDEX_ENTITY_NAME, self.index_name())
        data = self.index_value()
        document = self.payload
        return DynamoDBModel(pk, sk, data, document)


class QueryIndexByCollectionName(Query):

    def __init__(self, collection_name: str):
        super(QueryIndexByCollectionName, self).__init__(INDEX_ENTITY_NAME)
        super(QueryIndexByCollectionName, self).add_begins_with("collection.name", collection_name)


class QueryIndexByCollectionNameAndFields(Query):

    def __init__(self, collection_name: str, fields: List[str]):
        super(QueryIndexByCollectionNameAndFields, self).__init__(INDEX_ENTITY_NAME)
        super(QueryIndexByCollectionNameAndFields, self).add_eq("collection.name", collection_name)
        super(QueryIndexByCollectionNameAndFields, self).add_begins_with("fields", INDEX_FIELD_SEPARATOR.join(fields))


@dataclass(frozen=True)
class AggregationConfigurationEntity(Model):
    uid: uuid.UUID
    payload: dict = None

    @classmethod
    def from_dynamo_db_key(cls, last_evaluated_key: DynamoDBKey) -> str:
        return str.replace(last_evaluated_key.partition_key, AGGREGATION_CONFIGURATION_ENTITY_NAME + '#', '')

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> AggregationConfigurationEntity:
        return AggregationConfigurationEntity(
            uuid.UUID(str.replace(dynamo_db_model.pk, AGGREGATION_CONFIGURATION_ENTITY_NAME + '#', '')),
            dynamo_db_model.document)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        return convert_model_to_dynamo_db_item(self)

    def id(self):
        return str(self.uid)

    @classmethod
    def entity_name(cls):
        return "%s" % AGGREGATION_CONFIGURATION_ENTITY_NAME

    def ordering(self):
        return None

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

    @classmethod
    def entity_name(cls):
        return AGGREGATION_CONFIGURATION_ENTITY_NAME

    def index_name(self):
        return "collection.name"

    def index_value(self):
        return "{0}#{1}".format(self.collection_name, self.ordering)

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> AggregationConfigurationByCollectionNameEntity:
        data_split = dynamo_db_model.data.split("#")
        collection_name = data_split[0]
        _id = uuid.UUID(str.replace(dynamo_db_model.pk, AGGREGATION_CONFIGURATION_ENTITY_NAME + '#', ''))
        ordering = data_split[1]
        payload = dynamo_db_model.document
        return AggregationConfigurationByCollectionNameEntity(_id,
                                                              collection_name,
                                                              payload,
                                                              ordering)

    def to_dynamo_db_model(self) -> DynamoDBModel:
        pk = "{0}#{1}".format(AGGREGATION_CONFIGURATION_ENTITY_NAME, str(self.uid))
        sk = "{0}#{1}".format(AGGREGATION_CONFIGURATION_ENTITY_NAME, self.index_name())
        data = self.index_value()
        document = self.payload
        return DynamoDBModel(pk, sk, data, document)


class QueryAggregationConfigurationByCollectionName(Query):

    def __init__(self, collection_name: str):
        super(QueryAggregationConfigurationByCollectionName, self).__init__(AGGREGATION_CONFIGURATION_ENTITY_NAME)
        super(QueryAggregationConfigurationByCollectionName, self).add_begins_with("collection.name", collection_name)


@dataclass(frozen=True)
class AggregationEntity(Model):
    uid: uuid.UUID
    payload: dict = None

    @classmethod
    def from_dynamo_db_key(cls, last_evaluated_key: DynamoDBKey) -> str:
        return str.replace(last_evaluated_key.partition_key, AGGREGATION_ENTITY_NAME + '#', '')

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> AggregationEntity:
        return AggregationEntity(
            uuid.UUID(str.replace(dynamo_db_model.pk, AGGREGATION_ENTITY_NAME + '#', '')),
            dynamo_db_model.document)

    def to_dynamo_db_item(self) -> DynamoDBModel:
        return convert_model_to_dynamo_db_item(self)

    def id(self):
        return str(self.uid)

    @classmethod
    def entity_name(cls):
        return "%s" % AGGREGATION_ENTITY_NAME

    def ordering(self):
        return None

    def object(self):
        return self.payload


@dataclass(frozen=True)
class AggregationByAggregationConfigurationNameEntity(IndexModel):
    uid: uuid.UUID
    aggregation_configuration_name: str
    payload: dict
    ordering: str = "{0}".format(int(datetime.utcnow().timestamp()) * 1000)

    def id(self):
        return str(self.uid)

    @classmethod
    def entity_name(cls):
        return AGGREGATION_ENTITY_NAME

    def index_name(self):
        return "configuration_name"

    def index_value(self):
        return "{0}#{1}".format(self.aggregation_configuration_name, self.ordering)

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> AggregationByAggregationConfigurationNameEntity:
        data_split = dynamo_db_model.data.split("#")
        configuration_name = data_split[0]
        _id = uuid.UUID(str.replace(dynamo_db_model.pk, AGGREGATION_ENTITY_NAME + '#', ''))
        ordering = data_split[1]
        payload = dynamo_db_model.document
        return AggregationByAggregationConfigurationNameEntity(_id,
                                                               configuration_name,
                                                               payload,
                                                               ordering)

    def to_dynamo_db_model(self) -> DynamoDBModel:
        pk = "{0}#{1}".format(AGGREGATION_ENTITY_NAME, str(self.uid))
        sk = "{0}#{1}".format(AGGREGATION_ENTITY_NAME, self.index_name())
        data = self.index_value()
        document = self.payload
        return DynamoDBModel(pk, sk, data, document)


class QueryAggregationByAggregationConfigurationName(Query):

    def __init__(self, aggregation_configuration_name: str):
        super(QueryAggregationByAggregationConfigurationName, self).__init__(AGGREGATION_ENTITY_NAME)
        super(QueryAggregationByAggregationConfigurationName, self).add_begins_with("configuration_name",
                                                                                    aggregation_configuration_name)
