from __future__ import annotations
from typing import *

from enum import Enum

from dynamoplus.models.query.conditions import Predicate
from dynamoplus.utils.utils import auto_str


class AggregationType(str, Enum):
    #COUNT = "COUNT"
    COLLECTION_COUNT = "COLLECTION_COUNT"
    AVG = "AVG"
    #AVG_JOIN = "AVG_JOIN"
    SUM = "SUM"
    #SUM_COUNT = "SUM_COUNT"
    #MIN = "MIN"
    #MAX = "MAX"

    @classmethod
    def types(cls):
        return [t for t, v in cls.__members__.items()]

    @staticmethod
    def value_of(value) -> AggregationType:
        for m, mm in AggregationType.__members__.items():
            if m == value.upper():
                return mm


class AggregationTrigger(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

    @classmethod
    def types(cls):
        return [t for t, v in cls.__members__.items()]

    @staticmethod
    def value_of(value) -> AggregationTrigger:
        for m, mm in AggregationTrigger.__members__.items():
            if m == value.upper():
                return mm


@auto_str
class AggregationJoin(object):

    def __init__(self, collection_name: str, using_field: str):
        self.collection_name = collection_name
        self.using_field = using_field

    def __members(self):
        return self.collection_name, self.using_field

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__members() == other.__members()
        else:
            return False

    def __str__(self):
        return "{" + ",".join(map(lambda x: x.__str__(), self.__members())) + "}"

    def __hash__(self):
        return hash(self.__members())


@auto_str
class Aggregation(object):
    def __init__(self, name: str, configuration_name: str):
        self.name = name
        self.configuration_name = configuration_name

    def __members(self):
        return self.name, self.configuration_name

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__members() == other.__members()
        else:
            return False

    def __str__(self):
        return "{" + ",".join(map(lambda x: x.__str__(), self.__members())) + "}"

    def __hash__(self):
        return hash(self.__members())


@auto_str
class AggregationCount(Aggregation):
    count: int

    def __init__(self, name: str, configuration_name: str, count: int):
        super().__init__(name, configuration_name)
        self.count = count

    def __members(self):
        return self.name, self.configuration_name, self.count

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__members() == other.__members()
        else:
            return False

    def __str__(self):
        return "{" + ",".join(map(lambda x: x.__str__(), self.__members())) + "}"

    def __hash__(self):
        return hash(self.__members())


@auto_str
class AggregationSum(Aggregation):
    sum: int

    def __init__(self, name: str, configuration_name: str, sum: int):
        super().__init__(name, configuration_name)
        self.sum = sum

    def __members(self):
        return self.name, self.configuration_name, self.sum

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__members() == other.__members()
        else:
            return False

    def __str__(self):
        return "{" + ",".join(map(lambda x: x.__str__(), self.__members())) + "}"

    def __hash__(self):
        return hash(self.__members())

@auto_str
class AggregationAvg(Aggregation):
    avg: float

    def __init__(self, name: str, configuration_name: str, avg: float):
        super().__init__(name, configuration_name)
        self.avg = avg

    def __members(self):
        return self.name, self.configuration_name, self.avg

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__members() == other.__members()
        else:
            return False

    def __str__(self):
        return "{" + ",".join(map(lambda x: x.__str__(), self.__members())) + "}"

    def __hash__(self):
        return hash(self.__members())


@auto_str
class AggregationConfiguration(object):

    def __init__(self, collection_name: str, type: AggregationType, on: List[AggregationTrigger], target_field: str,
                 matches: Predicate, join: AggregationJoin):
        self.collection_name = collection_name
        self.type = type
        self.on = on
        self.target_field = target_field
        self.matches = matches
        self.join = join
        self.name = AggregationConfiguration.get_name(collection_name, type, target_field, matches, join)

    @staticmethod
    def get_name(collection_name: str, type: AggregationType, target_field: str,
                 matches: Predicate, join: AggregationJoin):
        matches_part = ""
        join_part = ""
        target_part = ""
        if target_field:
            target_part = "_{}".format(target_field)
        if matches:
            matches_part = "_{}".format("_".join(matches.get_fields() + matches.get_values()))
        if join:
            join_part = "by_{}".format(join.collection_name)
        return "{}{}_{}{}{}".format(collection_name, matches_part, type.name.lower(), target_part, join_part)

    def __members(self):
        return self.collection_name, self.type, self.on, self.target_field, self.matches, self.join, self.name

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__members() == other.__members()
        else:
            return False

    def __str__(self):
        return "{" + ",".join(map(lambda x: x.__str__(), self.__members())) + "}"

    def __hash__(self):
        return hash(self.__members())
