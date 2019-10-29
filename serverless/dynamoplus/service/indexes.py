from typing import *
from dynamoplus.models.indexes.indexes import Index
#from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.repository.repositories import DomainRepository
from dynamoplus.models.indexes.indexes import Query, Index
from dynamoplus.repository.models import QueryResult, Model


class IndexService(object):
    def __init__(self, collection:Collection, index:Index):
        self.index = index
        self.collection=collection
        
    
    def findDocuments(self,document:dict,startFrom:str=None, limit:int=None):
        repository = DomainRepository(self.collection)
        query = Query(document,self.index,startFrom,limit)
        queryResult = repository.find(query=query)
        return list(map(lambda r: r.fromDynamoDbItem(), queryResult.data)), queryResult.lastEvaluatedKey