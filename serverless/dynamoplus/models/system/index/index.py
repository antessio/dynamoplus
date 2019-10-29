from typing import *


class Index(object):
    def __init__(self,collectionName:str, conditions:List[str], orderingKey:str=None):
        self.collectionName = collectionName
        self.conditions = conditions
        self.orderingKey = orderingKey
    
    def conditions(self):
        return self.conditions
    
    def collectionName(self):
        return self.collectionName
    def indexName(self):
        return "__".join(self.conditions)+("__ORDER_BY__"+self.orderingKey if self.orderingKey is not None else "")
    
    def __str__(self):
        return "index={} collectionName={} conditions={} orderingKey={}".format(self.indexName(),self.collectionName, self.conditions, self.orderingKey)
