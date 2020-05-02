from typing import *
import abc

from dynamoplus.utils.utils import auto_str


@auto_str
class FieldMatch(object):

    @abc.abstractmethod
    def to_string(self):
        pass

    def get_fields(self):
        pass

    def is_range(self):
        return "range" in self.to_string()


@auto_str
class Predicate(FieldMatch):

    def get_values(self):
        pass


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

    def get_fields(self):
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

    def __init__(self, conditions: List[Predicate]):
        self.conditions = conditions

    def to_string(self):
        conditions_str = ""
        for c in self.conditions:
            conditions_str += "{}__".format(c.to_string())
        if conditions_str.endswith("__"):
            conditions_str = conditions_str[:-2]

        return "and({})".format(conditions_str)

    def get_fields(self):
        fields = []
        for c in self.conditions:
            fields.extend(c.get_fields())
        return fields

    def get_values(self):
        values = []
        for c in self.conditions:
            values.extend(c.get_values())
        return values

    def __eq__(self, o: object) -> bool:
        if isinstance(o, And):
            return self.conditions.__eq__(o.conditions)
        return super().__eq__(o)


def is_valid(field_match: FieldMatch):
    results = __get_range_conditions(field_match)
    return results is None or len(results) <= 1


def get_range_predicate(predicate: Predicate):
    results = __get_range_conditions(predicate)
    if results and len(results) >= 1:
        return results[0]


def __get_range_conditions(field_match: FieldMatch):
    if isinstance(field_match, Range):
        return [field_match]
    elif isinstance(field_match, And):
        result = []
        for c in field_match.conditions:
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
        for c in field_match.conditions:
            field_names.extend(get_field_names_in_order(c))
        return field_names
    return None
