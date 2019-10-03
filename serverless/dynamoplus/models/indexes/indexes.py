from typing import *


class Index(object):
    def __init__(self,entityName:str, conditions:List[str], orderingKey:str):
        self.entityName = entityName
        self.conditions = conditions
        self.orderingKey = orderingKey
    
    def conditions(self):
        return self.conditions
    
    def entityName(self):
        return self.entityName
    
    def indexName(self):
        return "__".join(self.conditions)+"__ORDER_BY__"+self.orderingKey

class Query(object):
    def __init__(self, document:dict,index:Index, startFrom:str=None, limit:int=50):
        self.document = document
        self.index = index
        self.startFrom = startFrom
        self.limit = limit
    