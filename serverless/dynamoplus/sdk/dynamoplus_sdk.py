import http.client as http_client
import getopt
import typing
import requests
import logging
import ssl
import json


http_client.HTTPConnection.debuglevel = 0
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.INFO)
requests_log.propagate = True

from enum import Enum


class Constraint(Enum):
    NULLABLE = 0
    NOT_NULL = 1


class AttributeType(Enum):
    STRING = 1
    NUMBER = 2
    OBJECT = 3
    ARRAY = 4


class Field(object):
    def __init__(self, attribute_name: str, attribute_type: AttributeType,
                 attribute_constraints: typing.List[Constraint] = []):
        self._attribute_name = attribute_name
        self._attribute_type = attribute_type
        self._attribute_constraints = attribute_constraints


    @property
    def attribute_name(self):
        return self._attribute_name

    @attribute_name.setter
    def attribute_name(self, value):
        self._attribute_name = value

    @property
    def attribute_type(self):
        return self._attribute_type

    @attribute_type.setter
    def attribute_type(self, value):
        self._attribute_type = value

    @property
    def attribute_constraints(self):
        return self._attribute_constraints

    @attribute_constraints.setter
    def attribute_constraints(self, value):
        self._attribute_constraints = value

class Index(object):
    def __init__(self, collection_name: str, conditions: typing.List[str], ordering_key: str = None):
        self._collection_name = collection_name
        self._conditions = conditions
        self._ordering_key = ordering_key

    @property
    def conditions(self):
        return self._conditions


    @property
    def collection_name(self):
        return self._collection_name

    @collection_name.setter
    def collection_name(self, value):
        self._collection_name = value

    @property
    def index_name(self):
        return "__".join(self._conditions) + (
            "__ORDER_BY__" + self._ordering_key if self._ordering_key is not None else "")


class Collection(object):
    _collection_name: str
    _id_key: str
    _fields: typing.List[Field]

    def __init__(self,collection_name:str, id_key:str, fields:typing.List[Field]=None):
        self._collection_name = collection_name
        self._id_key = id_key
        self._fields = fields


    @property
    def collection_name(self):
        return self._collection_name

    @collection_name.setter
    def collection_name(self, value):
        self._collection_name = value

    @property
    def id_key(self):
        return self._id_key

    @id_key.setter
    def id_key(self, value):
        self._id_key = value


class SDK(object):
    def __init__(self, host:str, environment:str=None):
        self.host = host
        self.environment = environment

    def create_collection(self, collection_name:str, id_key:str, ordering_key = None, fields: typing.List[Field] = None):
        conn = self.get_connection()
        data = {
            "name": collection_name,
            "id_key": id_key
        }
        if ordering_key:
            data["ordering_key"] = ordering_key

        # "fields": [
		# 	{
		# 		"name": "name",
		# 		"type": "string",
		# 		"constraints": ["unique","not_null"]
		# 	},
		# 	{
		# 		"name": "ordering",
		# 		"type": "string",
		# 		"constraints": ["not_null"]
		# 	}
		# ]

        # if fields:
        #     data["fields"] = {**(dict(name=f.attribute_name, type=f.attribute_type, constraints=[]) for f in fields)}
        json_data = json.dumps(data)
        headers = {'Content-type': 'application/json'}
        conn.request("POST", "/{}/dynamoplus/collection".format(self.environment), json_data, headers)
        res = conn.getresponse()
        logging.info("response status {}".format(res.status))
        response_body = res.read().decode()
        logging.info("response body {}".format(response_body))
        conn.close()
        collection = json.loads(response_body)
        return SDK.from_dict_to_collection(collection)




    def get_collection(self, collection_name : str):
        ## GET /dynamoplus/collection/<collection_name>
        connection = self.get_connection()
        connection.request("GET","/{}/dynamoplus/collection/{}".format(self.environment,collection_name))
        response = connection.getresponse()
        logging.info("response status {}".format(response.status))
        response_body = response.read().decode()
        logging.info("response body {}".format(response_body))
        result = json.loads(response_body)
        connection.close()
        return SDK.from_dict_to_collection(result)

    def create_index(self,index_name:str, collection_name:str, fields:typing.List[Field]):
        # POST /dynamoplus/index
        #{
        #     "collection": {
        #         "name": "category"
        #     },
        #     "name": "name" ????????
        # }
        connection = self.get_connection()
        index = {
            "collection":{
                "name": collection_name
            },
            "name": index_name,
            "fields": [f.__dict__ for f in fields]
        }
        json_data = json.dumps(index)
        headers = {'Content-type': 'application/json'}
        connection.request("POST", "/{}/dynamoplus/index".format(self.environment),json_data,headers)
        response = connection.getresponse()
        logging.info("response status {}".format(response.status))
        response_body = response.read().decode()
        logging.info("response body {}".format(response_body))
        result = json.loads(response_body)
        connection.close()
        return SDK.from_dict_to_index(result)
    def create_document(self,collection_name : str, document : dict):
        #POST /dynamoplus/<collection_name>
        #{
        # DOCUMENT
        #}
        connection = self.get_connection()
        json_data = json.dumps(document)
        headers = {'Content-type': 'application/json'}
        connection.request("POST", "/{}/dynamoplus/{}".format(self.environment,collection_name), json_data, headers)
        response = connection.getresponse()
        logging.info("response status {}".format(response.status))
        response_body = response.read().decode()
        logging.info("response body {}".format(response_body))
        result = json.loads(response_body)
        connection.close()
        return result
    def get_document(self, collection_name : str, id:str):
        # GET /dynamo_plus/<collection_name>/<id>
        connection = self.get_connection()
        connection.request("GET", "/{}/dynamoplus/{}/{}".format(self.environment,collection_name,id))
        response = connection.getresponse()
        logging.info("response status {}".format(response.status))
        response_body = response.read().decode()
        logging.info("response body {}".format(response_body))
        result = json.loads(response_body)
        connection.close()
        return result
    def query_document_by_index(self,collection_name:str, index_name:str, example_document:dict):
        # POST /dynamo_plus/<collection_name>/query/<query_id>
        #{ example }
        pass
    def query_all_documents(self,collection_name:str):
        # POST /dynamo_plus/<collection_name>/query
        #{}
        pass
    def delete_document(self, collection_name:str, id:str):
        # DELETE /dynamo_plus/<collection_name>/<id>
        connection = self.get_connection()
        connection.request("DELETE", "/{}/dynamoplus/{}/{}".format(self.environment,collection_name, id))
        response = connection.getresponse()
        logging.info("response status {}".format(response.status))
        connection.close()

    def get_connection(self):
        conn = http_client.HTTPSConnection(
            self.host,
            context=ssl._create_unverified_context()
        )
        return conn

    @staticmethod
    def from_dict_to_collection(collection_dict:dict):
        # TODO: fields
        return Collection(collection_dict["name"],collection_dict["id_key"],None)
    @staticmethod
    def from_dict_to_index(index_dict:dict):
        return Index(index_dict["collection_name"],index_dict["fields"],index_dict["ordering_key"] if "ordering_key" in index_dict else None)