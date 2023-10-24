from __future__ import annotations

import abc
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Type

from dynamoplus.models.query.conditions import Predicate


@dataclass(frozen=True)
class Counter:
    field_name: str
    count: Decimal
    is_increment: bool = True

@dataclass(frozen=True)
class CounterIncrement(abc.ABC):
    id: str
    field_name: str
    increment: Decimal

    @abc.abstractmethod
    def entity_name(self) -> str:
        raise NotImplementedError()

@dataclass(frozen=True)
class Model(abc.ABC):

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, document: dict) -> Model:
        raise NotImplementedError()

    @abc.abstractmethod
    def id(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def ordering(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def object(self) -> dict:
        raise NotImplementedError()

    @abc.abstractmethod
    def entity_name(self) -> str:
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

    @abc.abstractmethod
    def document(self):
        raise NotImplementedError()


class Condition(abc.ABC):
    field_name: str
    pass


class AnyCondition(Condition):
    pass


@dataclass
class AndCondition(Condition):
    eq_conditions: List[EqCondition]
    last_condition: Condition


@dataclass
class EqCondition(Condition):
    field_name: str
    field_value: str


@dataclass
class BeginsWithCondition(Condition):
    field_name: str
    field_value: str


@dataclass
class GtCondition(Condition):
    field_name: str
    field_value: str


@dataclass
class GteCondition(Condition):
    field_name: str
    field_value: str


@dataclass
class LtCondition(Condition):
    field_name: str
    field_value: str


@dataclass
class LteCondition(Condition):
    field_name: str
    field_value: str


@dataclass
class BetweenCondition(Condition):
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
    _conditions: List[Condition] = field(init=False, default=None)

    def __is_final(self):
        return self._between or self._begins_with or self._lt or self._lte or self._gt or self._gte

    def conditions(self) -> List[Condition]:
        return self._conditions

    def add_eq(self, field_name: str, field_value: str):
        if self._eq is None:
            self._eq = []
        condition = EqCondition(field_name, field_value)
        self._eq.append(condition)
        self._conditions.append(condition)

    def add_gt(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        condition = GtCondition(field_name, field_value)
        self._gt = condition
        self._conditions.append(condition)

    def add_gte(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        condition = GteCondition(field_name, field_value)
        self._gte = condition
        self._conditions.append(condition)

    def add_lt(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        condition = LtCondition(field_name, field_value)
        self._lt = condition
        self._conditions.append(condition)

    def add_lte(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        condition = LteCondition(field_name, field_value)
        self._lte = condition
        self._conditions.append(condition)

    def add_begins_with(self, field_name: str, field_value: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        condition = BeginsWithCondition(field_name, field_value)
        self._begins_with = condition
        self._conditions.append(condition)

    def add_between(self, field_name: str, field_value_from: str, field_value_to: str):
        if self.__is_final():
            raise RuntimeError("query is final can't be modified")
        condition = BetweenCondition(field_name, field_value_from, field_value_to)
        self._between = condition
        self._conditions.append(condition)

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
    def create(self, model: Model) -> dict:
        pass

    @abc.abstractmethod
    def get(self, id: str, entity_name: str) -> dict:
        pass

    @abc.abstractmethod
    def update(self, model: Model) -> dict:
        pass

    @abc.abstractmethod
    def delete(self, id: str, entity_name: str, ) -> None:
        pass

    @abc.abstractmethod
    def indexing(self, indexing: IndexingOperation) -> None:
        pass

    @abc.abstractmethod
    def query(self, entity_name: str, predicate: Condition, limit: int,
              starting_after: str = None) -> (List[dict], str):
        pass

    @abc.abstractmethod
    def increment_count(self, param:CounterIncrement):
        pass
