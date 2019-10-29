from typing import *
from enum import Enum

class AttributeConstraint(Enum):
    NULLABLE=0
    NOT_NULL=1

class AttributeType(Enum):
    STRING=1
    NUMBER=2
    OBJECT=3
    ARRAY=4

class AttributeDefinition(object):
    def __init__(self,attributeName:str,attributeType:AttributeType,attributeConstraints:List[AttributeConstraint]=[]):
        self.attributeName = attributeName
        self.attributeType = attributeType
        self.attributeConstraints = attributeConstraints

class Collection(object):
    def __init__(self,name:str, idKey:str, orderingKey:str=None, attributesDefinition:List[AttributeDefinition]=None):
        self.name = name
        self.idKey = idKey
        self.attributeDefinition=attributesDefinition
        self.orderingKey = orderingKey
        


    