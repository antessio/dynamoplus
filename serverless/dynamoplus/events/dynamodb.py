from typing import *
import logging
import os
import boto3
import json
from boto3.dynamodb.types import TypeDeserializer
from decimal import Decimal
from dynamoplus.http.handler.dynamoPlusHandler import DynamoPlusHandler, DynamoPlusHandlerInterface
from dynamoplus.repository.models import IndexModel
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.service.system.system import SystemService

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
            return {k: deserialize(v) for k, v in data.items()}
    else:
        return data


def dynamoStreamHandler(event, context):
    logger.info("Events on dynamo {} ".format(str(event)))

    for record in event.get('Records'):
        keys = record['dynamodb']['Keys']

        pk = keys['pk']['S']
        sk = keys['sk']['S']
        if "#" not in sk:
            system_service = SystemService()
            collection_metadata = system_service.get_collection_by_name(sk)

            if collection_metadata:
                if record.get('eventName') == 'INSERT':
                    new_record = deserialize(record['dynamodb']['NewImage'])
                    logger.info("creating index for {}".format(str(new_record)))
                    document = json.loads(new_record["document"],parse_float=Decimal)
                    indexing(lambda r: r.create(document), system_service, sk,
                             collection_metadata, document)

                elif record.get('eventName') == 'MODIFY':
                    new_record = deserialize(record['dynamodb']['NewImage'])
                    # document = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], new_record.items()))
                    logger.info("updating index for {}".format(str(new_record)))
                    document = json.loads(new_record["document"],parse_float=Decimal)
                    indexing(lambda r: r.update(document), system_service, sk,
                             collection_metadata, document)

                elif record.get('eventName') == 'REMOVE':
                    old_record = deserialize(record['dynamodb']['OldImage'])
                    logger.info('removing index on record  {}'.format(str(old_record)))
                    document = json.loads(old_record["document"],parse_float=Decimal)
                    id = document[collection_metadata.id_key]
                    indexing(lambda r: r.delete(id), system_service, sk,
                             collection_metadata, document)
            else:
                logger.debug('Skipping indexing on record {} - {}: entity not found'.format(pk, sk))
        else:
            logger.debug('Skipping indexing on record {} - {}'.format(pk, sk))


def indexing(repository_action: Callable[[DynamoPlusRepository],None], system_service: SystemService, collection_name: str,
             collection_metadata: Collection, new_record: dict):

    is_system = DynamoPlusHandler.is_system(collection_name)
    if not is_system:
        for index,last_evaluated_key in system_service.find_indexes_from_collection_name(collection_name):
            repository = IndexDynamoPlusRepository(collection_metadata,index,False)
            index_model = IndexModel(collection_metadata, new_record,index)
            if index_model.data():
                repository_action(repository)