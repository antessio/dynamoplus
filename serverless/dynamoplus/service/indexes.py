from typing import *
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.service.IndexService import IndexUtils


class IndexService(object):
    def __init__(self, index:Index):
        self.index = index
            
        # TODO: from index, build orderBy (if present, otherwise uses the entity orderingKey), and conditions
        # self.orderBy
        # self.conditions
        # TODO IndexRepository(documentTypeConfiguration, indexConfiguration)
        #self.repository = IndexRepository(index.entityName,index.indexName,self.orderBy,self.conditions)

    # def findByExample(self, entity:str, limit:int=None, startFrom:str=None):
    #     query={
    #         "entity": entity
    #     }
    #     if limit:
    #         query["limit"]=limit
    #     if self.orderBy is not None:
    #         query["orderBy"]=self.orderBy
    #     if startFrom:
    #         query["startFrom"] = startFrom
    #     return  self.repository.find(query)