from typing import *

from enum import Enum

from dynamoplus.utils.utils import auto_str


class AttributeConstraint(Enum):
    NULLABLE = "NULLABLE"
    NOT_NULL = "NOT_NULL"


class AttributeType(Enum):
    STRING = "STRING"
    NUMBER = "NUMBER"
    OBJECT = "OBJECT"
    ARRAY = "ARRAY"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"


SubAttribute = NewType("AttributeDefinition", object)


@auto_str
class AttributeDefinition(object):
    def __init__(self, name: str, type: AttributeType,
                 constraints: List[AttributeConstraint] = [],
                 attributes: List[SubAttribute] = []):
        self.name = name
        self.type = type
        self.constraints = constraints
        self.attributes = attributes




class Collection(object):
    def __init__(self, name: str, id_key: str, ordering_key: str = None,
                 attributes_definition: List[AttributeDefinition] = None,
                 auto_generate_id = False):
        self.name = name
        self.id_key = id_key
        self.attribute_definition = attributes_definition
        self.ordering_key = ordering_key
        self.auto_generate_id = auto_generate_id


    def __members(self):
        return self.name, self.id_key, self.attribute_definition, self.ordering_key, self.auto_generate_id

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__members() == other.__members()
        else:
            return False

    def __str__(self):
        return "{" + ",".join(map(lambda x: x.__str__(), self.__members()))+ "}"

    def __hash__(self):
        return hash(self.__members())

