import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class CustomService(object):

    def route(self, method: str, path:str, path_parameters: dict, query_parameters: dict = None, headers: dict = None,
              body: dict = None):
        response = {
            "method": method,
            "path":path,
            "path_parameters":path_parameters,
            "query_parameters":query_parameters,
            "heades":headers,
            "body":body
        }
        return {"statusCode": "200", "body": json.dumps(response)}
