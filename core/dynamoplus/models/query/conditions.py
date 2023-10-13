from dataclasses import dataclass
from typing import *
import abc

from dynamoplus.utils.utils import auto_str, get_values_by_key_recursive


@auto_str
class FieldMatch(object):

    @abc.abstractmethod
    def to_string(self):
        pass

    def get_fields(self) -> List[str]:
        pass

    def is_range(self):
        return "range" in self.to_string()


@auto_str
class Predicate(FieldMatch):

    def get_values(self) -> List[str]:
        pass


@dataclass(frozen=True)
class QueryCommand:
    predicate: Predicate
    index_name: str
    index_fields: List[str]


@auto_str
class AnyMatch(Predicate):
    def __init__(self):
        pass

    def to_string(self):
        return ""

    def get_fields(self):
        return []

    def get_values(self):
        return []

    def __eq__(self, o: object) -> bool:
        return isinstance(o, AnyMatch)


@auto_str
class Eq(Predicate):

    def __init__(self, field_name: str, value: str = None):
        self.field_name = field_name
        self.value = value

    def to_string(self):
        return "eq({})".format(self.field_name)

    def get_fields(self) -> List[str]:
        return [self.field_name]

    def get_values(self):
        return [self.value]

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

    def to_string(self):
        return "range({})".format(self.field_name)

    def get_fields(self):
        return [self.field_name]

    def get_values(self):
        return [self.from_value, self.to_value]

    def __eq__(self, o: object) -> bool:
        if isinstance(o, Range):
            return self.field_name == o.field_name and self.from_value == o.from_value and self.to_value == o.to_value
        return super().__eq__(o)


@auto_str
class And(Predicate):

    def __init__(self, eq_conditions: List[Eq], last_condition: Predicate = None):
        self.eq_conditions = eq_conditions
        if isinstance(last_condition, And) or isinstance(last_condition, AnyMatch):
            raise ValueError("last condition must not be And or AnyMatch")
        self.last_condition = last_condition

    def to_string(self):
        conditions_str = ""
        for c in self.eq_conditions:
            conditions_str += "{}__".format(c.to_string())
        if self.last_condition:
            conditions_str += "{}__".format(self.last_condition.to_string())

        if conditions_str.endswith("__"):
            conditions_str = conditions_str[:-2]

        return "and({})".format(conditions_str)

    def get_fields(self):
        fields = []
        for c in self.eq_conditions:
            fields.extend(c.get_fields())
        if self.last_condition:
            fields.extend(self.last_condition.get_fields())
        return fields

    def get_values(self):
        values = []
        for c in self.eq_conditions:
            values.extend(c.get_values())
        if self.last_condition:
            values.extend(self.last_condition.get_values())
        return values

    def __eq__(self, o: object) -> bool:
        if isinstance(o, And):
            return self.eq_conditions.__eq__(o.eq_conditions) and (
                    self.last_condition and self.last_condition.__eq__(o.last_condition))
        return super().__eq__(o)


def is_valid(field_match: FieldMatch):
    results = __get_range_conditions(field_match)
    return results is None or len(results) <= 1


def get_range_predicate(predicate: Predicate) -> Range:
    results = __get_range_conditions(predicate)
    if results and len(results) >= 1:
        for r in results:
            if isinstance(r, Range):
                return r


def __get_range_conditions(field_match: FieldMatch) -> List[Predicate]:
    if isinstance(field_match, Range):
        return [field_match]
    elif isinstance(field_match, And):
        result = []
        for c in field_match.eq_conditions:
            range_match_nested = get_range_predicate(c)
            if isinstance(range_match_nested, list):
                result.extend(range_match_nested)
            elif range_match_nested:
                result.append(range_match_nested)
        return result


def get_field_names_in_order(field_match: FieldMatch):
    if isinstance(field_match, Eq):
        return [field_match.field_name]
    elif isinstance(field_match, Range):
        return [field_match.field_name]
    elif isinstance(field_match, And):
        field_names = []
        for c in field_match.eq_conditions:
            field_names.extend(get_field_names_in_order(c))
        return field_names
    return None


def match_predicate(d: dict, predicate: Predicate):
    ## if and call recursively on all conditions
    ## if eq get the value from the field_name and compare with the field_value
    ## if range get the value from the field and compare with value1 and value2
    if isinstance(predicate, Eq):
        value = get_values_by_key_recursive(d, [predicate.field_name], True)
        if value and len(value) > 0:
            return value[0].__eq__(predicate.value)
    elif isinstance(predicate, Range):
        value = get_values_by_key_recursive(d, [predicate.field_name], True)
        if value and len(value) > 0:
            return predicate.from_value < value[0] < predicate.to_value
    elif isinstance(predicate, And):
        return all(map(lambda p: match_predicate(d, p), predicate.eq_conditions))

    return False
