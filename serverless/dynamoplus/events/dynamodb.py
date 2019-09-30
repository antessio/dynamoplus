import logging
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer
from dynamoplus.service.IndexService import IndexUtils,IndexService
from dynamoplus.repository.Repository import IndexRepository
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
    indexes = os.environ['INDEXES'].split(",")
    entities = os.environ['ENTITIES'].split(",")
    logger.info("Events on dynamo {} ".format(str(event)))
    systemEntitiesIndexService = IndexService(tableName, "entity", "entity#name",dynamodb)
    systemIndexesIndexService = IndexService(tableName, "index", "index#entity.name",dynamodb)
    for record in event.get('Records'):
        keys = record['dynamodb']['Keys']
        
        pk = keys['pk']['S']
        sk = keys['sk']['S']
        indexUtils = IndexUtils()
        if "#" not in sk:
            entityConfiguration = None
            systemEntitiesResult = systemEntitiesIndexService.findByExample({"name": sk})
            if "data" in systemEntitiesResult:
                if len(systemEntitiesResult["data"])>0:
                    entityConfiguration = systemEntitiesResult["data"][0]
                
            if record.get('eventName') == 'INSERT':
                newRecord = deserialize(record['dynamodb']['NewImage'])
                if entityConfiguration:
                    entity=entityConfiguration["name"]
                    idKey = entityConfiguration["idKey"]
                    customIndexes =  None
                    customIndexesResult = systemIndexesIndexService.findByExample({"entity":{"name": entity}})
                    if customIndexesResult:
                        if "data" in customIndexesResult:
                            if len(customIndexesResult["data"])>0:
                               customIndexes= ",".join(map(lambda i: i["entity"]["name"]+"#"+i["name"], customIndexesResult["data"]))
                    if customIndexes:
                        customMatchingIndexes = indexUtils.findIndexFromEntity(customIndexes,newRecord,sk)
                        for i in customMatchingIndexes.keys():
                            try:
                                logger.info("index found {}".format(i))
                                index = customMatchingIndexes[i]
                                logger.info("index tablePrefix {} and conditions {}".format(index["tablePrefix"], index["conditions"]))
                                indexKeys = index["conditions"]
                                logger.info("Index keys {}".format(indexKeys))
                                orderingKey = index["orderBy"] if "orderBy" in index else None
                                logger.info("Entity {} idKey {} orderingKey {}".format(entity,idKey, orderingKey))
                                repository =  IndexRepository(tableName,index["tablePrefix"],idKey,orderingKey,indexKeys,dynamoDB=dynamodb)
                                entity = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], newRecord.items()))
                                logger.info("creating index for {}".format(str(entity)))
                                repository.create(entity)
                            except Exception as e:
                                logger.warning("Unable to create the index {}".format(i),exc_info=e)

                    systemMatchingIndexes = indexUtils.findIndexFromEntity(indexes,newRecord,sk)
                    for i in systemMatchingIndexes.keys():
                        try:
                            logger.info("index found {}".format(i))
                            index = systemMatchingIndexes[i]
                            logger.info("index tablePrefix {} and conditions {}".format(index["tablePrefix"], index["conditions"]))
                            indexKeys = index["conditions"]
                            logger.info("Index keys {}".format(indexKeys))
                            orderingKey = index["orderBy"] if "orderBy" in index else None
                            logger.info("Entity {} idKey {} orderingKey {}".format(entity,idKey, orderingKey))
                            repository =  IndexRepository(tableName,index["tablePrefix"],idKey,orderingKey,indexKeys,dynamoDB=dynamodb)
                            entity = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], newRecord.items()))
                            logger.info("creating index for {}".format(str(entity)))
                            repository.create(entity)
                        except Exception as e:
                            logger.warning("Unable to create the index {}".format(i),exc_info=e)
                else:
                    logger.info("Unable to retrieve the entity {} ".format(sk))
                
            elif record.get('eventName') == 'MODIFY':
                newRecord = deserialize(record['dynamodb']['NewImage'])
                oldRecord = deserialize(record['dynamodb']['OldImage'])
                if entityConfiguration:
                    systemMatchingIndexes = indexUtils.findIndexFromEntity(indexes,newRecord,sk)
                    for i in systemMatchingIndexes.keys():
                        try:
                            logger.info("index found {}".format(i))
                            index = systemMatchingIndexes[i]
                            logger.info("index tablePrefix {} and conditions {}".format(index["tablePrefix"], index["conditions"]))
                            indexKeys = index["conditions"]
                            logger.info("Index keys {}".format(indexKeys))
                            entity=entityConfiguration["name"]
                            idKey = entityConfiguration["idKey"]
                            orderingKey = index["orderBy"] if "orderBy" in index else None
                            logger.info("Entity {} idKey {} orderingKey {}".format(entity,idKey, orderingKey))
                            repository =  IndexRepository(tableName,index["tablePrefix"],idKey,orderingKey,indexKeys,dynamoDB=dynamodb)
                            entity = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], newRecord.items()))
                            logger.info("updating index for {}".format(str(entity)))
                            repository.update(entity)
                        except Exception as e:
                            logger.warning("Unable to create the index {}".format(i),exc_info=e)
                else:
                    logger.info("Unable to retrieve the entity {} ".format(sk))
                
            elif record.get('eventName') == 'REMOVE':
                oldRecord = deserialize(record['dynamodb']['OldImage'])
                if entityConfiguration:
                    systemMatchingIndexes = indexUtils.findIndexFromEntity(indexes,oldRecord,sk)
                    for i in systemMatchingIndexes.keys():
                        try:
                            logger.info("index found {}".format(i))
                            index = systemMatchingIndexes[i]
                            logger.info("index tablePrefix {} and conditions {}".format(index["tablePrefix"], index["conditions"]))
                            indexKeys = index["conditions"]
                            logger.info("Index keys {}".format(indexKeys))
                            entity=entityConfiguration["name"]
                            idKey = entityConfiguration["idKey"]
                            orderingKey = index["orderBy"] if "orderBy" in index else None
                            logger.info("Entity {} idKey {} orderingKey {}".format(entity,idKey, orderingKey))
                            repository =  IndexRepository(tableName,index["tablePrefix"],idKey,orderingKey,indexKeys,dynamoDB=dynamodb)
                            entity = dict(filter(lambda kv: kv[0] not in ["geokey","hashkey"], oldRecord.items()))
                            logger.info('removing index on record  {}'.format(pk))
                            repository.delete(oldRecord[idKey])
                        except Exception as e:
                            logger.warning("Unable to create the index {}".format(i),exc_info=e)
                else:
                    logger.info("Unable to retrieve the entity {} ".format(sk))
        else:
            logger.debug('Skipping indexing on record {} - {}'.format(pk,sk))
