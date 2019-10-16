from typing import *
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.service.indexes import IndexService
from dynamoplus.repository.repositories import Repository
from dynamoplus.models.indexes.indexes import Query, Index
from dynamoplus.repository.models import QueryResult, Model


class DynamoPlusService(object):
    def __init__(self, systemDocumentTypesStr:str, systemIndexesStr:str):
        self.systemDocumentTypesStr = systemDocumentTypesStr
        self.systemIndexesStr = systemIndexesStr
        
    def getSystemDocumentTypeConfigurationFromDocumentType(self, documentType:str):
        systemDocumentTypeStr = next(filter(lambda tc: tc.split("#")[0]==documentType, self.systemDocumentTypesStr.split(",")),None)
        if systemDocumentTypeStr:
            systemDocumentTypeStrArray = systemDocumentTypeStr.split("#")
            return DocumentTypeConfiguration(documentType,systemDocumentTypeStrArray[1],systemDocumentTypeStrArray[2] if len(systemDocumentTypeStrArray)>1 else None)
    
    def getCustomDocumentTypeConfigurationFromDocumentType(self, documentType:str):
        documentTypeConfiguration = self.getSystemDocumentTypeConfigurationFromDocumentType("collection")
        index = Index(documentType,["name"])
        query = Query({"name": documentType},index)
        data, lastKey=IndexService(documentTypeConfiguration,index).findDocuments({"name": documentType})
        if len(data)>0:
            documenTypeDict = data[0]
            return DocumentTypeConfiguration(documentType,documenTypeDict["idKey"],documenTypeDict["orderingKey"])
    
    def getDocumentTypeConfigurationFromDocumentType(self, documentType:str):
        documentTypeConfiguration = self.getSystemDocumentTypeConfigurationFromDocumentType(documentType)
        if documentTypeConfiguration:
            return documentTypeConfiguration
        else:
            return self.getCustomDocumentTypeConfigurationFromDocumentType(documentType)
        
    def getIndexConfigurationsByDocumentType(self, documentType: str):
        documentTypeConfiguration = self.getSystemDocumentTypeConfigurationFromDocumentType(documentType)
        if documentTypeConfiguration:
            # should read the indexes from the environment variables
            indexesStrArray = self.systemIndexesStr.split(",")
            return list(filter(lambda i: i.documentType==documentType, map(lambda x:  DynamoPlusService.buildIndex(x),indexesStrArray)))
        else:
            indexByDocType=next(filter(lambda i: "collection.name" in i.conditions, self.getIndexConfigurationsByDocumentType("index")))
            print("Index for doc type")
            print(indexByDocType.documentType)
            indexService = IndexService(self.getSystemDocumentTypeConfigurationFromDocumentType("index"), indexByDocType)
            data, lastKey = indexService.findDocuments({"collection":{"name":documentType}})
            print("Indexes by doc type")
            return list(map(lambda d: DynamoPlusService.buildIndex(documentType+"#"+d["name"]), data))
            # for d in data:
            #     indexFound = 
            #     print(indexFound.conditions)
                # indexService = IndexService(self.getSystemDocumentTypeConfigurationFromDocumentType("index"),indexConfiguration)
                # data, lastKey = indexService.findDocuments({"collection":{"name":documentType}})
        
                # if len(data)>0:
                #     print(data)
                
    @staticmethod
    def buildIndex(indexStr:str):
        indexStrArray = indexStr.split("#")
        documentType = indexStrArray[0]
        indexName = indexStrArray[1]
        parts1 = indexName.split("__ORDER_BY__")
        conditions=parts1[0].split("__")
        return Index(documentType,conditions,orderingKey=parts1[1] if len(parts1)>1 else None)
        
    def getIndexServiceByIndex(self, documentType:str, indexName:str):
        documentTypeConfiguration = self.getDocumentTypeConfigurationFromDocumentType(documentType)
        if documentTypeConfiguration:
            parts1 = indexName.split("__ORDER_BY__")
            conditions=parts1[0].split("__")
            index = Index(documentType,conditions,orderingKey=parts1[1] if len(parts1)>1 else None)
            return IndexService(documentTypeConfiguration,index)