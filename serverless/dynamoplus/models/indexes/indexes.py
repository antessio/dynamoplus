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
    
class AbstractQuery(object):
    def __init__(self, startFrom:str=None, limit:int=50):
        self.startFrom = startFrom
        self.limit = limit
    def __str__(self):
        return "startFrom={}, limit={}".format(self.startFrom,self.limit)
class Query(AbstractQuery):
    def __init__(self, document:dict,index:Index, startFrom:str=None, limit:int=50):
        AbstractQuery.__init__(self,startFrom,limit)
        self.document = document
        self.index = index
    def __str__(self):
        return "index={}, startFrom={}, limit={}, document={}".format(self.index, self.startFrom,self.limit, self.document )

class RangeQuery(Query):
    def __init__(self, fromValue:str,toValue:str,index:Index, startFrom:str=None, limit:int=50):
        Query.__init__(self, None,index,startFrom,limit)
        self.fromValue = fromValue
        self.toValue = toValue
    def __str__(self):
        return "index={}, startFrom={}, limit={}, from={}, to={}".format(self.index, self.startFrom,self.limit, self.fromValue, self.toValue )