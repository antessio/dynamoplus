from __future__ import annotations

import abc
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import List, Type

from aws.dynamodb.dynamodbdao import DynamoDBDAO, DynamoDBModel, DynamoDBQuery, FIELD_SEPARATOR, DynamoDBKey, \
    GSIDynamoDBQuery, AtomicIncrement, Counter


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

    @classmethod
    @abc.abstractmethod
    def entity_name(cls):
        raise NotImplementedError()

    @abc.abstractmethod
    def ordering(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def object(self):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def from_dynamo_db_key(cls, last_evaluated_key: DynamoDBKey) -> str:
        pass

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

    @classmethod
    @abc.abstractmethod
    def entity_name(cls):
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
    def to_dynamo_db_model(self) -> DynamoDBModel:
        raise NotImplementedError()


class Condition(abc.ABC):

    @abc.abstractmethod
    def to_dynamo_db(self) -> DynamoDBQuery:
        pass


@dataclass
class EqCondition:
    field_name: str
    field_value: str


@dataclass
class BeginsWithCondition:
    field_name: str
    field_value: str


@dataclass
class GtCondition:
    field_name: str
    field_value: str


@dataclass
class GteCondition:
    field_name: str
    field_value: str


@dataclass
class LtCondition:
    field_name: str
    field_value: str


@dataclass
class LteCondition:
    field_name: str
    field_value: str


@dataclass
class BetweenCondition:
    field_name: str
    field_value_from: str
    field_value_to: str


@dataclass
class Query(abc.ABC):
    collection_name: str
    _eq: List[EqCondition] = field(init=False, default=None)
    _begins_with: BeginsWithCondition = field(init=False, default=None)
    _lt: LtCondition = field(init=False, default=None)
    _lte: LteCondition = field(init=False, default=None)
    _gt: GtCondition = field(init=False, default=None)
    _gte: GteCondition = field(init=False, default=None)
    _between: BetweenCondition = field(init=False, default=None)

    def __is_final(self):
        return self._between or self._begins_with or self._lt or self._lte or self._gt or self._gte

    def add_eq(self, field_name: str, field_value: str):
        if self._eq is None:
            self._eq = []
        self._eq.append(EqCondition(field_name, field_value))

    def add_gt(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        self._gt = GtCondition(field_name, field_value)

    def add_gte(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        self._gte = GteCondition(field_name, field_value)

    def add_lt(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        self._lt = LtCondition(field_name, field_value)

    def add_lte(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        self._lte = LteCondition(field_name, field_value)

    def add_begins_with(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        self._begins_with = BeginsWithCondition(field_name, field_value)

    def add_between(self, field_name: str, field_value_from: str, field_value_to: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        self._between = BetweenCondition(field_name, field_value_from, field_value_to)

    def build_dynamo_query(self) -> DynamoDBQuery:
        dynamo_db_query = GSIDynamoDBQuery()
        field_names, field_values = self.__extract_fields_eq()
        if self._lt:
            field_names.append(self._lt.field_name)
            field_values.append(self._lt.field_value)
            partition_key, sort_key = self.__build_keys(field_names, field_values)
            dynamo_db_query.lt(self.collection_name + "#" + partition_key, sort_key)
        elif self._gt:
            field_names.append(self._gt.field_name)
            field_values.append(self._gt.field_value)
            partition_key, sort_key = self.__build_keys(field_names, field_values)
            dynamo_db_query.gt(self.collection_name + "#" + partition_key, sort_key)
        elif self._lte:
            field_names.append(self._lte.field_name)
            field_values.append(self._lte.field_value)
            partition_key, sort_key = self.__build_keys(field_names, field_values)
            dynamo_db_query.lte(self.collection_name + "#" + partition_key, sort_key)
        elif self._gte:
            field_names.append(self._gte.field_name)
            field_values.append(self._gte.field_value)
            partition_key, sort_key = self.__build_keys(field_names, field_values)
            dynamo_db_query.gte(self.collection_name + "#" + partition_key, sort_key)
        elif self._begins_with:
            field_names.append(self._begins_with.field_name)
            field_values.append(self._begins_with.field_value)
            partition_key, sort_key = self.__build_keys(field_names, field_values)
            dynamo_db_query.begins_with(self.collection_name + "#" + partition_key, sort_key)
        elif self._between:
            field_names.append(self._between.field_name)
            partition_key, sort_key = self.__build_keys(field_names, field_values)
            sort_key_from = sort_key + FIELD_SEPARATOR + self._between.field_value_from
            sort_key_to = sort_key + FIELD_SEPARATOR + self._between.field_value_to
            dynamo_db_query.between(self.collection_name + "#" + partition_key, sort_key_from, sort_key_to)
        else:
            if len(field_names) != 0:
                partition_key, sort_key = self.__build_keys(field_names, field_values)
                dynamo_db_query.eq(self.collection_name + "#" + partition_key, sort_key)
            else:
                dynamo_db_query.all(self.collection_name)

        return dynamo_db_query

    @staticmethod
    def __build_keys(field_names, field_values):
        partition_key = FIELD_SEPARATOR.join(field_names)
        sort_key = FIELD_SEPARATOR.join(field_values)
        return partition_key, sort_key

    def __extract_fields_eq(self):
        field_names = []
        field_values = []
        if self._eq:
            for eq_condition in self._eq:
                field_names.append(eq_condition.field_name)
                field_values.append(eq_condition.field_value)
        return field_names, field_values


class QueryAll(Query):

    def __init__(self, entity_type: Type[Model]):
        super(QueryAll, self).__init__(entity_type.entity_name())


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

    @abc.abstractmethod
    def query(self, query: Query, limit: int, starting_after: Model = None) -> (List[Model], Model):
        pass


class DynamoDBRepository(RepositoryInterface):

    def __init__(self, table_name: str, model_class: Type[Model]):
        self.dao = DynamoDBDAO(table_name)
        self.model_class = model_class

    def create(self, model: Model):
        result = self.dao.create(model.to_dynamo_db_item())
        return self.model_class.from_dynamo_db_item(result)

    def update(self, model: Model) -> Model:
        result = self.dao.update(model.to_dynamo_db_item())
        return self.model_class.from_dynamo_db_item(result)

    def get(self, model: Model) -> Model:
        dynamo_db_model = model.to_dynamo_db_item()
        result = self.dao.get(dynamo_db_model.pk, dynamo_db_model.sk)
        return self.model_class.from_dynamo_db_item(result)

    def delete(self, model: Model):
        dynamo_db_model = model.to_dynamo_db_item()
        self.dao.delete(dynamo_db_model.pk, dynamo_db_model.sk)

    def indexing(self, indexing: IndexingOperation) -> None:
        for r in indexing.to_delete:
            model = r.to_dynamo_db_model()
            self.dao.delete(model.pk, model.sk)
        for r in indexing.to_update:
            model = r.to_dynamo_db_model()
            self.dao.update(model)
        for r in indexing.to_create:
            model = r.to_dynamo_db_model()
            self.dao.create(model)

    def query(self, query: Query, limit: int, starting_after: Model = None) -> (List[Model], str):
        start_from = None
        if starting_after:
            starting_after_dynamo_db_model = starting_after.to_dynamo_db_item()
            start_from = DynamoDBKey(starting_after_dynamo_db_model.pk, starting_after_dynamo_db_model.sk,
                                     starting_after_dynamo_db_model.data)
        result = self.dao.query(query.build_dynamo_query(), limit, start_from)
        last_evaluated_model = None
        if result.lastEvaluatedKey:
            last_evaluated_model = self.model_class.from_dynamo_db_key(result.lastEvaluatedKey)
        return list(map(lambda d: self.model_class.from_dynamo_db_item(d), result.data)), last_evaluated_model

    def increment_counter(self, model: Model, counters: List[Counter]):
        self.dao.increment_counter(AtomicIncrement(model.to_dynamo_db_item().pk, model.to_dynamo_db_item().sk, counters))


def convert_model_to_dynamo_db_item(model: Model):
    pk = model.entity_name() + "#" + model.id()
    sk = model.entity_name()
    data = model.ordering() if model.ordering() else model.id()
    document = model.object()
    return DynamoDBModel(pk, sk, data, document)
