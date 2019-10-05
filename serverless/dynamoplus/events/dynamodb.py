from typing import *
import logging
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer
from dynamoplus.service.dynamoplus import DynamoPlusService
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.repository.models import IndexModel
from dynamoplus.repository.repositories import IndexRepository

logger = logging.getLogger()
logger.setLevel(logging.INFO)

serializer = TypeDeserializer()
dynamodb = boto3.resource("dynamodb")
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

def dynamoStreamHandler(event, context):
    tableName = os.environ['DYNAMODB_TABLE']
    indexes = os.environ['INDEXES']
    entities = os.environ['ENTITIES']
    logger.info("Events on dynamo {} ".format(str(event)))

    dynamoPlusService = DynamoPlusService(entities,indexes)
    for record in event.get('Records'):
        keys = record['dynamodb']['Keys']
        
        pk = keys['pk']['S']
        sk = keys['sk']['S']
        if "#" not in sk:
            documentTypeConfiguration = dynamoPlusService.getDocumentTypeConfigurationFromDocumentType(sk)
            if documentTypeConfiguration:
                if record.get('eventName') == 'INSERT':
                    newRecord = deserialize(record['dynamodb']['NewImage'])
                    #document = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], newRecord.items()))
                    logger.info("creating index for {}".format(str(newRecord)))
                    repository = indexing(lambda r: r.create(newRecord), dynamoPlusService, sk, documentTypeConfiguration, newRecord)
            
                elif record.get('eventName') == 'MODIFY':
                    newRecord = deserialize(record['dynamodb']['NewImage'])
                    #document = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], newRecord.items()))
                    logger.info("updating index for {}".format(str(newRecord)))
                    indexing(lambda r: r.update(newRecord), dynamoPlusService, sk, documentTypeConfiguration, newRecord)
                    
                elif record.get('eventName') == 'REMOVE':
                    oldRecord = deserialize(record['dynamodb']['OldImage'])
                    logger.info('removing index on record  {}'.format(pk))
                    indexing(lambda r: r.delete(oldRecord[documentTypeConfiguration.idKey]), dynamoPlusService, sk, documentTypeConfiguration, oldRecord)
            else:
                logger.debug('Skipping indexing on record {} - {}: entity not found'.format(pk,sk))    
        else:
            logger.debug('Skipping indexing on record {} - {}'.format(pk,sk))

def indexing(repositoryAction, dynamoPlusService, sk, documentTypeConfiguration, newRecord):
    for index in dynamoPlusService.getIndexConfigurationsByDocumentType(sk):
        repository = IndexRepository(documentTypeConfiguration,index)
        repositoryAction(repository)



# def handleIndexes(repositoryLambda, documentConfiguration, systemIndexesIndexService, indexUtils, newRecord, sk, tableName, indexes):
#     if documentConfiguration:
#         documentTypeName=documentConfiguration["name"]
#         idKey = documentConfiguration["idKey"]
#         customIndexes =  None
#         logger.info("Search custom indexes for document {}".format(documentTypeName))
#         customIndexesResult = systemIndexesIndexService.findByExample({"document_type":{"name": documentTypeName}})
#         if customIndexesResult:
#             if "data" in customIndexesResult:
#                 logger.info("Found {} for document {}".format(len(customIndexesResult["data"]),documentTypeName))
#                 if len(customIndexesResult["data"])>0:
#                     customIndexes= list(map(lambda i: i["document_type"]["name"]+"#"+i["name"], customIndexesResult["data"]))
#         if customIndexes:
#             logger.info("Custom indexes string {}".format(customIndexes))
#             customMatchingIndexes = indexUtils.findIndexFromEntity(customIndexes,newRecord,sk)
#             for i in customMatchingIndexes.keys():
#                 try:
#                     logger.info("index found {}".format(i))
#                     index = customMatchingIndexes[i]
#                     logger.info("index tablePrefix {} and conditions {}".format(index["tablePrefix"], index["conditions"]))
#                     indexKeys = index["conditions"]
#                     logger.info("Index keys {}".format(indexKeys))
#                     orderingKey = index["orderBy"] if "orderBy" in index else None
#                     logger.info("Document {} idKey {} orderingKey {}".format(documentTypeName,idKey, orderingKey))
#                     repository =  IndexRepository(tableName,index["tablePrefix"],idKey,orderingKey,indexKeys,dynamoDB=dynamodb)
#                     repositoryLambda(repository,idKey)
#                 except Exception as e:
#                     logger.warning("Unable to create the index {}".format(i),exc_info=e)

#         logger.info("System indexes string {}".format(indexes))
#         systemMatchingIndexes = indexUtils.findIndexFromEntity(indexes,newRecord,sk)
#         logger.info("Found {} indexes matching the record for system indexes".format(len(systemMatchingIndexes)))
#         for i in systemMatchingIndexes.keys():
#             try:
#                 logger.info("index found {}".format(i))
#                 index = systemMatchingIndexes[i]
#                 logger.info("index tablePrefix {} and conditions {}".format(index["tablePrefix"], index["conditions"]))
#                 indexKeys = index["conditions"]
#                 logger.info("Index keys {}".format(indexKeys))
#                 orderingKey = index["orderBy"] if "orderBy" in index else None
#                 logger.info("Entity {} idKey {} orderingKey {}".format(documentTypeName,idKey, orderingKey))
#                 repository =  IndexRepository(tableName,index["tablePrefix"],idKey,orderingKey,indexKeys,dynamoDB=dynamodb)
#                 repositoryLambda(repository,idKey)
#             except Exception as e:
#                 logger.warning("Unable to create the index {}".format(i),exc_info=e)
#     else:
#         logger.info("Unable to retrieve the document_type {} ".format(sk))