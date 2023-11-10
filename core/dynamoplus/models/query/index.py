import abc
from dataclasses import dataclass
from typing import List

from dynamoplus.utils.utils import auto_str


@auto_str
class FieldMatch(object):
    pass


@auto_str
class Predicate(FieldMatch):

    def get_field_names(self) -> List[str]:
        pass


@dataclass(frozen=True)
class Query:
    matches: Predicate


@auto_str
class Eq(Predicate):

    def __init__(self, field_name: str, value: str = None):
        self.field_name = field_name
        self.value = value

    def get_field_names(self) -> List[str]:
        return [self.field_name]

    def to_string(self):
        return "eq({})".format(self.field_name)

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Eq):
            return self.field_name == o.field_name and self.value == o.value
        return super().__eq__(o)


@auto_str
class Range(Predicate):

    def __init__(self, field_name: str, from_value: str = None, to_value: str = None):
        self.field_name = field_name
        self.from_value = from_value
        self.to_value = to_value

    def get_field_names(self) -> List[str]:
        return [self.field_name]

    def to_string(self):
        return "range({})".format(self.field_name)

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Range):
            return self.field_name == o.field_name and self.from_value == o.from_value and self.to_value == o.to_value
        return super().__eq__(o)


@auto_str
class And(Predicate):

    def __init__(self, predicates: List[Predicate]):
        self.predicates = predicates

    def get_field_names(self) -> List[str]:
        fields = []
        for c in self.predicates:
            fields.extend(c.get_field_names())
        return fields
