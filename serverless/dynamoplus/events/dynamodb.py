from typing import *
import logging
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer
from dynamoplus.service.dynamoplus import DynamoPlusService
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.repository.models import IndexModel
from dynamoplus.repository.repositories import IndexDynamoPlusRepository

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
    tableName = os.environ['DYNAMODB_DOMAIN_TABLE']
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
                    indexing(lambda r: r.delete(oldRecord[documentTypeConfiguration.id_key]), dynamoPlusService, sk, documentTypeConfiguration, oldRecord)
            else:
                logger.debug('Skipping indexing on record {} - {}: entity not found'.format(pk,sk))    
        else:
            logger.debug('Skipping indexing on record {} - {}'.format(pk,sk))

def indexing(repositoryAction, dynamoPlusService, sk, documentTypeConfiguration, newRecord):
    for index in dynamoPlusService.getIndexConfigurationsByDocumentType(sk):
        repository = IndexDynamoPlusRepository(documentTypeConfiguration,index)
        indexModel = IndexModel(documentTypeConfiguration,newRecord,index)
        if indexModel.data():
            ## if new record doesn't contain the key should skip repositoryAction
            repositoryAction(repository)