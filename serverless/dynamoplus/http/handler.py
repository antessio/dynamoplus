from dynamoplus.utils.decimalencoder import DecimalEncoder
from dynamoplus.repository.Repository import Repository
from dynamoplus.service.IndexService import IndexService
from decimal import Decimal
from datetime import datetime
import uuid
import logging
import json
import os
import json

logging.basicConfig(level=logging.INFO)

class HttpHandler(object):
    def __init__(self, documentConfiguration,dynamoTable,dynamoDB=None):
        self.documentConfigurations = documentConfiguration.split(",")
        self.dynamoTable = dynamoTable
        self.dynamoDB = dynamoDB
    def get(self, pathParameters, queryStringParameters=[], body=None, headers=None):
        id = pathParameters['id']
        targetEntity = self._getTargetEntity(pathParameters)
        targetConfiguration = self._getTargetEntityConfiguration(targetEntity)
        if not targetConfiguration:
            return {
                "statusCode": 400,
                "body": self._formatJson({"msg": "entity {} not handled".format(targetEntity)})
            }
        repository = self._getRepositoryFromTargetEntityConfiguration(targetConfiguration)
        logging.info("get {} by id {}".format(targetEntity,id))
        result = repository.get(id)
        if result:
            dto = repository.getEntityDTO(result)
            return {"statusCode": 200, "body": self._formatJson(dto)}
        else:
            return {"statusCode": 404}

    def create(self, pathParameters, queryStringParameters=[], body=None, headers=None):
        targetEntity = self._getTargetEntity(pathParameters)
        targetConfiguration = self._getTargetEntityConfiguration(targetEntity)
        if not targetConfiguration:
            return {
                "statusCode": 400,
                "body": self._formatJson({"msg": "entity {} not handled".format(targetEntity)})
            }
        repository = self._getRepositoryFromTargetEntityConfiguration(targetConfiguration)
        data = json.loads(body.replace("'", '"'),parse_float=Decimal)
        timestamp = datetime.utcnow()
        uid=str(uuid.uuid1())
        data[targetConfiguration["idKey"]]=uid
        data["creation_date_time"]=timestamp.isoformat()
        logging.info("Creating "+data.__str__())
        try:
            data = repository.create(data)
            dto = repository.getEntityDTO(data)
            return {"statusCode": 201, "body": self._formatJson(dto)}
        except Exception as e:
            logging.error("Unable to create entity {} for body {}".format(targetEntity,body))
            logging.exception(str(e))
            return {"statusCode": 500, "body": self._formatJson({"msg": "Error in create entity {}".format(targetEntity)})}
    
    def update(self, pathParameters, queryStringParameters=[], body=None, headers=None):
        targetEntity = self._getTargetEntity(pathParameters)
        targetConfiguration = self._getTargetEntityConfiguration(targetEntity)
        if not targetConfiguration:
            return {
                "statusCode": 400,
                "body": self._formatJson({"msg": "entity {} not handled".format(targetEntity)})
            }
        
        repository = self._getRepositoryFromTargetEntityConfiguration(targetConfiguration)
        data = json.loads(body.replace("'", '"'),parse_float=Decimal)
        timestamp = datetime.utcnow()
        data["update_date_time"]=timestamp.isoformat()
        logging.info("Updating  "+data.__str__())
        try:
            data = repository.update(data)
            dto = repository.getEntityDTO(data)
            return {"statusCode": 200, "body": self._formatJson(dto)}
        except Exception as e:
            logging.error("Unable to update entity {} for body {}".format(targetEntity,body))
            logging.exception(str(e))
            return {"statusCode": 500, "body": self._formatJson({"msg": "Error in update entity {}".format(targetEntity)})}
    
    def delete(self, pathParameters, queryStringParameters=[], body=None, headers=None):
        id = pathParameters['id']
        targetEntity = self._getTargetEntity(pathParameters)
        targetConfiguration = self._getTargetEntityConfiguration(targetEntity)
        if not targetConfiguration:
            return {
                "statusCode": 400,
                "body": self._formatJson({"msg": "entity {} not handled".format(targetEntity)})
            }
        repository = self._getRepositoryFromTargetEntityConfiguration(targetConfiguration)
        logging.info("delete {} by id {}".format(targetEntity,id))
        try:
            repository.delete(id)
            return {"statusCode": 200}
        except Exception as e:
            logging.error("Unable to delete entity {} for body {}".format(targetEntity,body))
            logging.exception(str(e))
            return {"statusCode": 500, "body": self._formatJson({"msg": "Error in delete entity {}".format(targetEntity)})}

    def query(self, pathParameters, queryStringParameters={}, body=None, headers=None):
        targetEntity = self._getTargetEntity(pathParameters)
        targetConfiguration = self._getTargetEntityConfiguration(targetEntity)
        if not targetConfiguration:
            return {
                "statusCode": 400,
                "body": self._formatJson({"msg": "entity {} not handled".format(targetEntity)})
            }
        queryId = pathParameters['queryId']
        queryIndex = targetEntity+"#"+queryId
        logging.info("Received {} as index".format(queryIndex))
        
        entityName = targetConfiguration["name"]
        indexService = IndexService(self.dynamoTable, entityName, queryIndex,self.dynamoDB)
        entity=json.loads(body.replace("'", '"'),parse_float=Decimal)
        limit = None
        startFrom = None
        if queryStringParameters is not None and "limit" in queryStringParameters:
            limit=queryStringParameters["limit"]
        if "startFrom" in headers:
            startFrom = headers["startFrom"]
        result = indexService.findByExample(entity,limit,startFrom)
        return {
            "statusCode": 200,
            "body": self._formatJson(result)
        }

    def _formatJson(self, obj):
        return json.dumps(obj, cls=DecimalEncoder)
    def _getTargetEntityConfiguration(self, targetEntity):
        targetConfiguration = next(filter(lambda tc: tc.split("#")[0]==targetEntity, self.documentConfigurations),None)
        if targetConfiguration:
            logging.info("Accessing to system entity {}".format(targetConfiguration))
            targetConfigurationArray=targetConfiguration.split("#")
            return {"name": targetConfigurationArray[0], "idKey": targetConfigurationArray[1], "orderingKey": targetConfigurationArray[2]}
        else:
            indexService = IndexService(self.dynamoTable, "entity", "entity#name",self.dynamoDB)
            result = indexService.findByExample({"name": targetEntity})
            if "data" in result:
                if len(result["data"])>0:
                    return result["data"][0]
            return None

    def _getTargetEntity(self, pathParameters):
        targetEntity = pathParameters['entity']
        return targetEntity

    def _getRepositoryFromTargetEntityConfiguration(self, targetConfiguration):
        entity=targetConfiguration["name"]
        idKey = targetConfiguration["idKey"]
        orderingKey = targetConfiguration["orderingKey"]
        return self._getRepository(entity, idKey,orderingKey)
    def _getRepository(self, entity, idKey, orderingKey):
        return Repository(self.dynamoTable,entity, idKey,orderingKey,dynamoDB=self.dynamoDB)