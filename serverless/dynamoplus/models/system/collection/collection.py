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



@auto_str
class Collection(object):
    def __init__(self, name: str, id_key: str, ordering_key: str = None,
                 attributes_definition: List[AttributeDefinition] = None,
                 auto_generate_id = False):
        self.name = name
        self.id_key = id_key
        self.attribute_definition = attributes_definition
        self.ordering_key = ordering_key
        self.auto_generate_id = auto_generate_id
