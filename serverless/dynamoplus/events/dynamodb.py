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
                    try:
                        #repository = indexing(lambda r: r.create(newRecord), dynamoPlusService, sk, documentTypeConfiguration, newRecord)
                        repository = indexing(dynamoPlusService, sk, documentTypeConfiguration, newRecord)
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
                        repository = indexing(dynamoPlusService, sk, documentTypeConfiguration, newRecord)
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
                            repository = indexing(dynamoPlusService, sk, documentTypeConfiguration, newRecord)
                            if repository:
                                repository.delete(id)
                        except Exception as e:
                            logger.error("Error in delete {}".format(str(e)))
            else:
                logger.debug('Skipping indexing on record {} - {}: entity not found'.format(pk,sk))    
        else:
            logger.debug('Skipping indexing on record {} - {}'.format(pk,sk))

def indexing(dynamoPlusService, sk, documentTypeConfiguration, newRecord):
    document=newRecord["document"]
    for index in dynamoPlusService.getIndexConfigurationsByDocumentType(sk):
        logger.info("indexing {}  by {} ".format(str(document),str(index)))
        repository = IndexRepository(documentTypeConfiguration,index)
        indexModel = IndexModel(documentTypeConfiguration,document,index)
        if indexModel.data():
            ## if new record doesn't contain the key should skip indexing
            return repository
        