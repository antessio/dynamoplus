from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import List

from aws.dynamodb.dynamodbdao import DynamoDBDAO, DynamoDBModel


@dataclass(frozen=True)
class Model(abc.ABC):

    @classmethod
    @abc.abstractmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> Model:
        raise NotImplementedError()

    @abc.abstractmethod
    def to_dynamo_db_item(self) -> DynamoDBModel:
        raise NotImplementedError()

    @abc.abstractmethod
    def id(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def entity_name(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def ordering(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def object(self):
        raise NotImplementedError()


@dataclass(frozen=True)
class IndexingOperation:
    to_delete: List[IndexModel]
    to_update: List[IndexModel]
    to_create: List[IndexModel]


@dataclass(frozen=True)
class IndexModel(abc.ABC):
    @abc.abstractmethod
    def id(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def entity_name(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def index_name(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def index_value(self):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def from_dynamo_db_item(cls, dynamo_db_model: DynamoDBModel) -> IndexModel:
        raise NotImplementedError()

    @abc.abstractmethod
    def to_dynamo_db_item(self) -> DynamoDBModel:
        raise NotImplementedError()


class RepositoryInterface(abc.ABC):

    @abc.abstractmethod
    def create(self, model: Model) -> Model:
        pass

    @abc.abstractmethod
    def get(self, model: Model) -> Model:
        pass

    @abc.abstractmethod
    def update(self, model: Model) -> Model:
        pass

    @abc.abstractmethod
    def delete(self, model: Model) -> None:
        pass

    @abc.abstractmethod
    def indexing(self, indexing: IndexingOperation) -> None:
        pass


class DynamoDBRepositoryRepository(RepositoryInterface):

    def __init__(self, table_name: str):
        self.dao = DynamoDBDAO(table_name)

    def create(self, model: Model):
        result = self.dao.create(model.to_dynamo_db_item())
        return model.__class__.from_dynamo_db_item(result)

    def update(self, model: Model) -> Model:
        result = self.dao.update(model.to_dynamo_db_item())
        return model.__class__.from_dynamo_db_item(result)

    def get(self, model: Model) -> Model:
        dynamo_db_model = model.to_dynamo_db_item()
        result = self.dao.get(dynamo_db_model.pk, dynamo_db_model.sk)
        return model.__class__.from_dynamo_db_item(result)

    def delete(self, model: Model):
        dynamo_db_model = model.to_dynamo_db_item()
        self.dao.delete(dynamo_db_model.pk, dynamo_db_model.sk)

    def indexing(self, indexing: IndexingOperation) -> None:
        for r in indexing.to_delete:
            model = r.to_dynamo_db_item()
            self.dao.delete(model.pk, model.sk)
        for r in indexing.to_update:
            model = r.to_dynamo_db_item()
            self.dao.update(model)
        for r in indexing.to_create:
            model = r.to_dynamo_db_item()
            self.dao.create(model)


def convert_model_to_dynamo_db_item(model: Model):
    pk = model.entity_name() + "#" + model.id()
    sk = model.entity_name()
    data = model.ordering() if model.ordering() else model.id()
    document = model.object()
    return DynamoDBModel(pk, sk, data, document)
