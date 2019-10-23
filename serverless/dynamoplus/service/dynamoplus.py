from typing import *
import logging

from dynamoplus.models.indexes.indexes import Index
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.service.indexes import IndexService
from dynamoplus.repository.repositories import Repository
from dynamoplus.models.indexes.indexes import Query, Index
from dynamoplus.repository.models import QueryResult, Model,IndexModel
from dynamoplus.repository.repositories import IndexRepository

from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

serializer = TypeDeserializer()

class DynamoPlusService(object):
    def __init__(self, systemDocumentTypesStr:str, systemIndexesStr:str):
        self.systemDocumentTypesStr = systemDocumentTypesStr
        self.systemIndexesStr = systemIndexesStr
        
    def indexing(self, sk:str, documentTypeConfiguration:DocumentTypeConfiguration, newRecord:dict):
        document=newRecord["document"]
        for index in self.getIndexConfigurationsByDocumentType(sk):
            logger.info("indexing {}  by {} ".format(str(document),str(index)))
            repository = IndexRepository(documentTypeConfiguration,index)
            indexModel = IndexModel(documentTypeConfiguration,document,index)
            if indexModel.data():
                ## if new record doesn't contain the key should skip indexing
                return repository

    def recordIndexing(self, record:dict):
        keys = record['dynamodb']['Keys']
        
        pk = keys['pk']['S']
        sk = keys['sk']['S']
        if "#" not in sk:
            documentTypeConfiguration = self.getDocumentTypeConfigurationFromDocumentType(sk)
            if documentTypeConfiguration:
                if record.get('eventName') == 'INSERT':
                    newRecord = deserialize(record['dynamodb']['NewImage'])
                    #document = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], newRecord.items()))
                    logger.info("creating index for {}".format(str(newRecord)))
                    try:
                        #repository = indexing(lambda r: r.create(newRecord), dynamoPlusService, sk, documentTypeConfiguration, newRecord)
                        repository = self.indexing(sk, documentTypeConfiguration, newRecord)
                        if repository:
                            repository.create(newRecord["document"])
                    except Exception as e:
                        logger.error("Error in create {}".format(str(e)))
            
                elif record.get('eventName') == 'MODIFY':
                    newRecord = deserialize(record['dynamodb']['NewImage'])
                    #document = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], newRecord.items()))
                    logger.info("updating index for {}".format(str(newRecord)))
                    try:
                        #indexing(lambda r: r.update(newRecord), dynamoPlusService, sk, documentTypeConfiguration, newRecord)
                        repository = self.indexing(sk, documentTypeConfiguration, newRecord)
                        if repository:
                            repository.update(newRecord["document"])
                    except Exception as e:
                        logger.error("Error in update {}".format(str(e)))
                    
                elif record.get('eventName') == 'REMOVE':
                    oldRecord = deserialize(record['dynamodb']['OldImage'])
                    logger.info('removing index on record  {}'.format(pk))
                    if documentTypeConfiguration.idKey in oldRecord:
                        id=oldRecord["document"][documentTypeConfiguration.idKey]
                        try:
                            #indexing(lambda r: r.delete(id), dynamoPlusService, sk, documentTypeConfiguration, oldRecord)
                            repository = self.indexing(sk, documentTypeConfiguration, newRecord)
                            if repository:
                                repository.delete(id)
                        except Exception as e:
                            logger.error("Error in delete {}".format(str(e)))
            else:
                logger.debug('Skipping indexing on record {} - {}: entity not found'.format(pk,sk))    
        else:
            logger.debug('Skipping indexing on record {} - {}'.format(pk,sk))
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
            index=None
            if indexName: 
                parts1 = indexName.split("__ORDER_BY__")
                conditions=parts1[0].split("__")
                index = Index(documentType,conditions,orderingKey=parts1[1] if len(parts1)>1 else None)
            return IndexService(documentTypeConfiguration,index)


def deserialize(data):
    if isinstance(data, list):
       return [deserialize(v) for v in data]

    if isinstance(data, dict):
        try: 
            return serializer.deserialize(data)
        except TypeError:
            return { k : deserialize(v) for k, v in data.items() }
    else:
        return data

