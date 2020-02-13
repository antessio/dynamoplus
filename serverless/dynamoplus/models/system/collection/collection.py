from typing import *
from enum import Enum

from dynamoplus.utils.utils import auto_str


class AttributeConstraint(Enum):
    NULLABLE = 0
    NOT_NULL = 1


class AttributeType(Enum):
    STRING = 1
    NUMBER = 2
    OBJECT = 3
    ARRAY = 4

@auto_str
class AttributeDefinition(object):
    def __init__(self, attribute_name: str, attribute_type: AttributeType,
                 attribute_constraints: List[AttributeConstraint] = []):
        self.attribute_name = attribute_name
        self.attribute_type = attribute_type
        self.attribute_constraints = attribute_constraints

@auto_str
class Collection(object):
    def __init__(self, name: str, id_key: str, ordering_key: str = None,
                 attributes_definition: List[AttributeDefinition] = None):
        self.name = name
        self.id_key = id_key
        self.attribute_definition = attributes_definition
        self.ordering_key = ordering_key
