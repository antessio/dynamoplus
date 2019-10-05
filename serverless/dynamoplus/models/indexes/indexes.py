from typing import *


class Index(object):
    def __init__(self,documentType:str, conditions:List[str], orderingKey:str=None):
        self.documentType = documentType
        self.conditions = conditions
        self.orderingKey = orderingKey
    
    def conditions(self):
        return self.conditions
    
    def documentType(self):
        return self.documentType
    def indexName(self):
        return "__".join(self.conditions)+"__ORDER_BY__"+self.orderingKey
    
    def __str__(self):
        return "documentType={} conditions={} orderingKey={}".format(self.documentType, self.conditions, self.orderingKey)
    
class Query(object):
    def __init__(self, document:dict,index:Index, startFrom:str=None, limit:int=50):
        self.document = document
        self.index = index
        self.startFrom = startFrom
        self.limit = limit
    def __str__(self):
        return "index={}, startFrom={}, limit={}, document={}".format(self.index, self.startFrom,self.limit, self.document )