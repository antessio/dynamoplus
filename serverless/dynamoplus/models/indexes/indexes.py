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

    