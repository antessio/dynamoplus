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
        
    def getDocumentTypeConfigurationFromDocumentType(self, documentType:str):
        systemDocumentTypeStr = next(filter(lambda tc: tc.split("#")[0]==documentType, self.systemDocumentTypesStr.split(",")),None)
        if systemDocumentTypeStr:
            systemDocumentTypeStrArray = systemDocumentTypeStr.split("#")
            return DocumentTypeConfiguration(documentType,systemDocumentTypeStrArray[1],systemDocumentTypeStrArray[2] if len(systemDocumentTypeStrArray)>1 else None)

    def getRepositoryByDocumentType(self, documentType:str):
        documentTypeConfiguration = self.getDocumentTypeConfigurationFromDocumentType(documentType)
        if documentTypeConfiguration:
            return Repository(documentTypeConfiguration)
        else:
            documentTypeConfiguration = self.getDocumentTypeConfigurationFromDocumentType("document_type")
            index = Index(documentType,["name"])
            query = Query({"name": documentType},index)
            data, lastKey=IndexService(documentTypeConfiguration,index).findDocument({"name": documentType})
            if len(data)>0:
                documenTypeDict = data[0]
                return Repository(DocumentTypeConfiguration(documentType,documenTypeDict["idKey"],documenTypeDict["orderingKey"]))
        
        
    def getIndexServiceByIndex(self, documentType:str, indexName:str):
        documentTypeConfiguration = self.getDocumentTypeConfigurationFromDocumentType("document_type")
        parts1 = indexName.split("__ORDER_BY__")
        conditions=parts1[0].split("__")
        index = Index(documentType,conditions,orderingKey=parts1[1] if len(parts1)>1 else None)
        return IndexService(documentTypeConfiguration,index)