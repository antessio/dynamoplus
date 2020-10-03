import logging
import boto3
import json
from boto3.dynamodb.types import TypeDeserializer
from decimal import Decimal
from dynamoplus.service.indexing_service import create_indexes, update_indexes, delete_indexes

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


def dynamo_stream_handler(event, context):
    logger.info("Events on dynamo {} ".format(str(event)))

    for record in event.get('Records'):
        keys = record['dynamodb']['Keys']

        pk = keys['pk']['S']
        sk = keys['sk']['S']
        if "#" not in sk:

            if record.get('eventName') == 'INSERT':
                new_record = deserialize(record['dynamodb']['NewImage'])
                logger.info("creating index for {}".format(str(new_record)))
                new_document = json.loads(new_record["document"], parse_float=Decimal)
                create_indexes(sk, new_document,None)
                # create_indexes_for_collection(sk,new_document, lambda r: r.create(new_document))

            elif record.get('eventName') == 'MODIFY':
                new_record = deserialize(record['dynamodb']['NewImage'])
                old_record = deserialize(record['dynamodb']['OldImage'])
                # new_document = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], new_record.items()))
                logger.info("updating index for {}".format(str(new_record)))
                old_document = json.loads(old_record["document"], parse_float=Decimal)
                new_document = json.loads(new_record["document"], parse_float=Decimal)
                update_indexes(sk, new_document,old_document)
                # create_indexes_for_collection(sk, new_document, lambda r: r.update(new_document))

            elif record.get('eventName') == 'REMOVE':
                old_record = deserialize(record['dynamodb']['OldImage'])
                logger.info('removing index on record  {}'.format(str(old_record)))
                old_document = json.loads(old_record["document"], parse_float=Decimal)
                delete_indexes(sk, old_document)
                # id = new_document[collection_metadata.id_key]
                # indexing(lambda r: r.delete(id), system_service, sk,
                #          collection_metadata, new_document)
        else:
            logger.debug('Skipping indexing on record {} - {}'.format(pk, sk))
