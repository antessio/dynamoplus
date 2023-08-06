import os
from abc import ABC
from dataclasses import dataclass

from aws.dynamodb.dynamodbdao import DynamoDBKey, DynamoDBModel
from dynamoplus.v2.repository.repositories_v2 import Model, Query


@dataclass(frozen=True)
class DomainEntity(Model):
    id: str
    collection_name: str
    object: dict = None
    ordering: str = None

    @classmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> Model:
        id = dynamo_db_model.pk.split("#")[1]
        collection_name = dynamo_db_model.pk.split("#")[0]
        object = dynamo_db_model.document
        return DomainEntity(id, collection_name, object)

    def to_dynamo_db_model(self) -> DynamoDBModel:
        pk = self.collection_name + "#" + self.id
        sk = self.collection_name
        data = self.ordering if self.ordering else self.id
        document = self.object
        return DynamoDBModel(pk, sk, data, document)

    def id(self) -> str:
        return self.id

    @classmethod
    def entity_name(cls):
        raise NotImplementedError("entity name is unknown")

    def ordering(self) -> str:
        return self.ordering

    def object(self) -> dict:
        return self.object

    @classmethod
    def from_dynamo_db_key(cls, last_evaluated_key: DynamoDBKey) -> str:
        return last_evaluated_key.partition_key.split("#")[0]

    @classmethod
    def table_name(cls):
        return os.environ['DYNAMODB_DOMAIN_TABLE']


class QueryByField(Query):
    def __init__(self, collection_name: str):
        super(QueryByField, self).__init__(collection_name)