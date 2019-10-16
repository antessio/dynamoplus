from dynamoplus.utils.decimalencoder import DecimalEncoder
from dynamoplus.service.indexes import IndexService
from dynamoplus.models.documents.documentTypes import DocumentTypeConfiguration
from dynamoplus.models.indexes.indexes import Index
from dynamoplus.service.indexes import IndexService
from dynamoplus.service.dynamoplus import DynamoPlusService
from dynamoplus.repository.repositories import Repository
from decimal import Decimal
from datetime import datetime
import typing
import uuid
import logging
import json
import os
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class HttpHandler(object):
    def __init__(self):
        self.dynamoService = DynamoPlusService(os.environ["ENTITIES"],os.environ["INDEXES"])
    def get(self, pathParameters, queryStringParameters=[], body=None, headers=None):
        id = pathParameters['id']
        documentType = self.getDocumentTypeFromPathParameters(pathParameters)
        documentTypeConfiguration = self.dynamoService.getDocumentTypeConfigurationFromDocumentType(documentType)
        if not documentTypeConfiguration:
            return self.getHttpResponse(headers=self.getResponseHeaders(headers), statusCode=400,body=self.formatJson({"msg": "entity {} not handled".format(documentType)}))
        repository = Repository(documentTypeConfiguration)
        logger.info("get {} by id {}".format(documentType,id))
        result = repository.get(id)
        if result:
            dto = result.fromDynamoDbItem()
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=200,body=self.formatJson(dto))
        else:
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=404)

    def create(self, pathParameters, queryStringParameters=[], body=None, headers=None):
        documentType = self.getDocumentTypeFromPathParameters(pathParameters)
        documentTypeConfiguration = self.dynamoService.getDocumentTypeConfigurationFromDocumentType(documentType)
        if not documentTypeConfiguration:
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=400,body=self.formatJson({"msg": "entity {} not handled".format(documentType)}))
        logger.info("Creating {} ".format(documentType))
        repository = Repository(documentTypeConfiguration)
        data = json.loads(body,parse_float=Decimal)
        timestamp = datetime.utcnow()
        uid=str(uuid.uuid1())
        data[documentTypeConfiguration.idKey]=uid
        data["creation_date_time"]=timestamp.isoformat()
        logger.info("Creating "+data.__str__())
        try:
            data = repository.create(data)
            dto = data.fromDynamoDbItem()
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=201,body=self.formatJson(dto))
        except Exception as e:
            logger.error("Unable to create entity {} for body {}".format(documentType,body))
            logger.exception(str(e))
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=500,body=self.formatJson({"msg": "Error in create entity {}".format(documentType)}))
    
    def update(self, pathParameters, queryStringParameters=[], body=None, headers=None):
        documentType = self.getDocumentTypeFromPathParameters(pathParameters)
        documentTypeConfiguration = self.dynamoService.getDocumentTypeConfigurationFromDocumentType(documentType)
        if not documentTypeConfiguration:
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=400,body=self.formatJson({"msg": "entity {} not handled".format(documentType)}))
        logger.info("updating {} ".format(documentType))
        repository = Repository(documentTypeConfiguration)
        data = json.loads(body,parse_float=Decimal)
        timestamp = datetime.utcnow()
        data["update_date_time"]=timestamp.isoformat()
        logger.info("Updating  "+data.__str__())
        try:
            data = repository.update(data)
            dto = dto = data.fromDynamoDbItem()
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=200,body=self.formatJson(dto))
        except Exception as e:
            logger.error("Unable to update entity {} for body {}".format(documentType,body))
            logger.exception(str(e))
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=500,body=self.formatJson({"msg": "Error in update entity {}".format(documentType)}))
    
    def delete(self, pathParameters, queryStringParameters=[], body=None, headers=None):
        id = pathParameters['id']
        documentType = self.getDocumentTypeFromPathParameters(pathParameters)
        documentTypeConfiguration = self.dynamoService.getDocumentTypeConfigurationFromDocumentType(documentType)
        if not documentTypeConfiguration:
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=400,body=self.formatJson({"msg": "entity {} not handled".format(documentType)}))
        repository = Repository(documentTypeConfiguration)
        logger.info("delete {} by id {}".format(documentType,id))
        try:
            repository.delete(id)
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=200)
        except Exception as e:
            logger.error("Unable to delete entity {} for body {}".format(documentType,body))
            logger.exception(str(e))
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=500,body=self.formatJson({"msg": "Error in delete entity {}".format(documentType)}))

    def query(self, pathParameters, queryStringParameters={}, body=None, headers=None):
        documentType = self.getDocumentTypeFromPathParameters(pathParameters)
        documentTypeConfiguration = self.dynamoService.getDocumentTypeConfigurationFromDocumentType(documentType)
        if not documentTypeConfiguration:
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=400,body=self.formatJson({"msg": "entity {} not handled".format(documentType)}))
        repository = Repository(documentTypeConfiguration)
        queryId = pathParameters['queryId'] if 'queryId' in pathParameters  else None
        logger.info("Received {} as index".format(queryId))
        indexService = self.dynamoService.getIndexServiceByIndex(documentType, queryId)
        if not indexService:
            return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=400,body=self.formatJson({"msg": "query {} not handled".format(queryId)}))

        document=json.loads(body,parse_float=Decimal)
        limit = None
        startFrom = None
        if queryStringParameters is not None and "limit" in queryStringParameters:
            limit=queryStringParameters["limit"]
        if "startFrom" in headers:
            startFrom = headers["startFrom"]
        data, lastEvaluatedKey = indexService.findDocuments(document,startFrom,limit)
        return self.getHttpResponse(headers=self.getResponseHeaders(headers),statusCode=200,body=self.formatJson({"data": data,"lastKey": lastEvaluatedKey}))

    def checkAllowedOrigin(self,origin):
        allowedOrigins = os.environ["ALLOWED_ORIGINS"].split(",")
        return origin in allowedOrigins
    def getResponseHeaders(self, requestHeaders):
        responseHeaders = {}
        if requestHeaders and "Origin" in requestHeaders:
            origin = requestHeaders["Origin"]
            if self.checkAllowedOrigin(origin):
                responseHeaders["Access-Control-Allow-Origin"]=origin
        return responseHeaders
    def getHttpResponse(self,**kwargs):
        return {
            **kwargs
            }
    def formatJson(self, obj):
        return json.dumps(obj, cls=DecimalEncoder)
    def getTargetEntityConfiguration(self, targetEntity):
        targetConfiguration = next(filter(lambda tc: tc.split("#")[0]==targetEntity, self.documentConfigurations),None)
        if targetConfiguration:
            logger.info("Accessing to system entity {}".format(targetConfiguration))
            targetConfigurationArray=targetConfiguration.split("#")
            return {"name": targetConfigurationArray[0], "idKey": targetConfigurationArray[1], "orderingKey": targetConfigurationArray[2]}
        else:
            indexService = IndexService(self.dynamoTable, "entity", "entity#name",self.dynamoDB)
            result = indexService.findByExample({"name": targetEntity})
            if "data" in result:
                if len(result["data"])>0:
                    return result["data"][0]
            return None
    
    def getDocumentTypeFromPathParameters(self, pathParameters):
        if "collection" in pathParameters:
            targetEntity = pathParameters['collection']
            return targetEntity

    def getRepositoryFromTargetEntityConfiguration(self, targetConfiguration):
        entity=targetConfiguration["name"]
        idKey = targetConfiguration["idKey"]
        orderingKey = targetConfiguration["orderingKey"]
        return self.getRepository(entity, idKey,orderingKey)
    def getRepository(self, entity, idKey, orderingKey):
        return Repository(self.dynamoTable,entity, idKey,orderingKey,dynamoDB=self.dynamoDB)

    def getDocumentTypeConfiguration(self, targetEntity):
        targetConfiguration = self.getTargetEntityConfiguration(targetEntity)
        if not targetConfiguration:
            '''
                find the entity 
            '''
            systemDocumentTypesIndexService = IndexService(self.dynamoTable, "collection", "collection#name",self.dynamoDB)
            documentTypesResult = systemDocumentTypesIndexService.findByExample({"name": targetEntity})
            logger.info("Response is {}".format(str(documentTypesResult)))
            if "data" in documentTypesResult:
                if len(documentTypesResult["data"])>0:
                    targetConfiguration = documentTypesResult["data"][0]
        return targetConfiguration
