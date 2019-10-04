from typing import *
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.service.IndexService import IndexUtils
from dynamoplus.repository.repositories import Repository
from dynamoplus.models.indexes.indexes import Query, Index
from dynamoplus.repository.models import QueryResult, Model


class IndexService(object):
    def __init__(self, documentTypeConfiguration:DocumentTypeConfiguration, index:Index):
        self.index = index
        self.documentTypeConfiguration=documentTypeConfiguration
        
    
    def findDocument(self,document:dict,startFrom:str=None, limit:int=None):
        repository = Repository(self.documentTypeConfiguration)
        query = Query(document,self.index,startFrom,limit)
        queryResult = repository.find(query=query)
        return list(map(lambda r: r.fromDynamoDbItem(), queryResult.data)), queryResult.lastEvaluatedKey