import json
import logging
import os
import base64

from decimal import Decimal
from dynamoplus.http.handler.dynamoPlusHandler import DynamoPlusHandler, HandlerException
from dynamoplus.utils.decimalencoder import DecimalEncoder

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


## TODO : it has to be converter in "Router"

class HttpHandler(object):
    def __init__(self):
        # self.dynamoService = DynamoPlusService(os.environ["ENTITIES"],os.environ["INDEXES"])
        self.dynamoPlusHandler = DynamoPlusHandler()

    def get(self, path_parameters, query_string_parameters=[], body=None, headers=None):
        id = path_parameters['id']
        collection = self.get_document_type_from_path_parameters(path_parameters)
        logger.info("get {} by id {}".format(collection, id))
        try:
            result = self.dynamoPlusHandler.get(collection, id)
            if result:
                return self.get_http_response(headers=self.get_response_headers(headers), statusCode=200,
                                              body=self.format_json(result))
            else:
                return self.get_http_response(headers=self.get_response_headers(headers), statusCode=404)
        except HandlerException as e:
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=400,
                                          body=self.format_json({"msg": e.message}))

    def create(self, path_parameters, query_string_parameters=[], body=None, headers=None):
        collection = self.get_document_type_from_path_parameters(path_parameters)
        logger.info("Creating {}".format(collection))
        data = json.loads(body, parse_float=Decimal)
        logger.info("Creating " + data.__str__())
        try:
            dto = self.dynamoPlusHandler.create(collection, data)
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=201,
                                          body=self.format_json(dto))
        except HandlerException as e:
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=400,
                                          body=self.format_json({"msg": e.message}))
        except Exception as e:
            logger.error("Unable to create entity {} for body {}".format(collection, body))
            logger.exception(str(e))
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=500,
                                          body=self.format_json(
                                              {"msg": "Error in create entity {}".format(collection)}))

    def update(self, path_parameters: dict, queryStringParameters: list = [], body: dict = None,
               headers: dict = None) -> dict:
        collection = self.get_document_type_from_path_parameters(path_parameters)
        logger.info("Updating {}".format(collection))
        data = json.loads(body, parse_float=Decimal)
        logger.info("Updating " + data.__str__())
        try:
            dto = self.dynamoPlusHandler.update(collection, data)
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=200,
                                          body=self.format_json(dto))
        except HandlerException as e:
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=400,
                                          body=self.format_json({"msg": e.message}))
        except Exception as e:
            logger.error("Unable to update entity {} for body {}".format(collection, body))
            logger.exception(str(e))
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=500,
                                          body=self.format_json(
                                              {"msg": "Error in create entity {}".format(collection)}))

    def delete(self, path_parameters, queryStringParameters=[], body=None, headers=None):
        id = path_parameters['id']
        collection = self.get_document_type_from_path_parameters(path_parameters)
        logger.info("delete {} by id {}".format(collection, id))
        try:
            self.dynamoPlusHandler.delete(collection, id)
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=200)
        except HandlerException as e:
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=400,
                                          body=self.format_json({"msg": e.message}))

    def query(self, path_parameters, query_string_parameters={}, body=None, headers=None):
        logger.info("headers received {}".format(str(headers)))
        collection = self.get_document_type_from_path_parameters(path_parameters)
        logger.debug("query string parameters {}".format(query_string_parameters))
        query_id = path_parameters['queryId'] if 'queryId' in path_parameters else None
        logger.info("Received {} as index".format(query_id))
        query = json.loads(body, parse_float=Decimal)
        logger.debug("example query {}".format(query))
        last_key = query["start_from"] if "start_from" in query else None
        limit = int(query["limit"]) if query and "limit" in query else None
        document = query["matches"] if "matches" in query else {}
        logger.debug("last_key = {}".format(last_key))
        logger.debug("limit = {}".format(limit))
        try:
            documents, last_evaluated_key = self.dynamoPlusHandler.query(collection, query_id, document, last_key,
                                                                         limit)
            result = {"data": documents}
            if last_evaluated_key:
                result["last_key"] = last_evaluated_key
            return self.get_http_response(body=self.format_json(result), headers=self.get_response_headers(headers),
                                          statusCode=200)
        except HandlerException as e:
            return self.get_http_response(headers=self.get_response_headers(headers), statusCode=400,
                                          body=self.format_json({"msg": e.message}))
        # limit = None
        # startFrom = None
        # if query_string_parameters is not None and "limit" in query_string_parameters:
        #     limit = query_string_parameters["limit"]
        # if "startFrom" in headers:
        #     startFrom = headers["startFrom"]
        # data, lastEvaluatedKey = indexService.find_documents(query, startFrom, limit)
        # return self.get_http_response(headers=self.get_response_headers(headers), statusCode=200,
        #                               body=self.format_json({"data": data, "lastKey": lastEvaluatedKey}))

    def check_allowed_origin(self, origin):
        allowed_origins = os.environ["ALLOWED_ORIGINS"].split(",")
        return origin in allowed_origins

    def get_response_headers(self, request_headers):
        response_headers = {}
        if request_headers and "origin" in request_headers:
            origin = request_headers["origin"]
            if self.check_allowed_origin(origin):
                response_headers["Access-Control-Allow-Origin"] = origin
                response_headers["Access-Control-Allow-Credentials"] = True
        return response_headers

    @staticmethod
    def get_http_response(**kwargs):
        return {
            **kwargs
        }

    @staticmethod
    def format_json(obj):
        return json.dumps(obj, cls=DecimalEncoder)

    # def getTargetEntityConfiguration(self, targetEntity):
    #     targetConfiguration = next(filter(lambda tc: tc.split("#")[0] == targetEntity, self.documentConfigurations),
    #                                None)
    #     if targetConfiguration:
    #         logger.info("Accessing to system entity {}".format(targetConfiguration))
    #         targetConfigurationArray = targetConfiguration.split("#")
    #         return {"name": targetConfigurationArray[0], "idKey": targetConfigurationArray[1],
    #                 "orderingKey": targetConfigurationArray[2]}
    #     else:
    #         indexService = IndexService(self.dynamoTable, "entity", "entity#name", self.dynamoDB)
    #         result = indexService.findByExample({"name": targetEntity})
    #         if "data" in result:
    #             if len(result["data"]) > 0:
    #                 return result["data"][0]
    #         return None

    @staticmethod
    def get_document_type_from_path_parameters(path_parameters: dict) -> str:
        if "collection" in path_parameters:
            return path_parameters['collection']
