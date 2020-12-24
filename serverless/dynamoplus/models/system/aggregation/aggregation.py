from typing import *

from enum import Enum

from dynamoplus.models.query.conditions import Predicate
from dynamoplus.utils.utils import auto_str


class AggregationType(str, Enum):
    COLLECTION_COUNT = "COLLECTION_COUNT"
    AVG = "AVG"
    AVG_JOIN = "AVG_JOIN"
    SUM = "SUM"
    SUM_COUNT = "SUM_COUNT"
    MIN = "MIN"
    MAX = "MAX"

    @classmethod
    def types(cls):
        return [t for t,v in cls.__members__.items()]


class AggregationTrigger(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

    @classmethod
    def types(cls):
        return [t for t, v in cls.__members__.items()]


@auto_str
class AggregationJoin(object):

    def __init__(self, collection_name: str, using_field: str):
        self.collection_name = collection_name
        self.using_field = using_field


@auto_str
class Aggregation(object):

    def __init__(self, collection_name: str, type: AggregationType, on: List[AggregationTrigger], target_field: str,
                 matches: Predicate, join: AggregationJoin):
        self.collection_name = collection_name
        self.type = type
        self.on = on
        self.target_field = target_field
        self.matches = matches
        self.join = join

    def name(self):
        matches_part = ""
        join_part = ""
        target_part = ""
        if self.target_field:
            target_part = "_{}".format(self.target_field)
        if self.matches:
            matches_part = "_{}".format("_".join(self.matches.get_fields() + self.matches.get_values()))
        if self.join:
            join_part = "by_{}".format("_".join(self.join.collection_name))
        return "{}{}_{}{}{}".format(self.collection_name, matches_part, self.type.name.lower(), target_part, join_part)

    def __members(self):
        return self.collection_name, self.type,self.on,self.target_field,self.matches,self.join

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__members() == other.__members()
        else:
            return False

    def __str__(self):
        return "{" + ",".join(map(lambda x: x.__str__(), self.__members())) + "}"

    def __hash__(self):
        return hash(self.__members())
