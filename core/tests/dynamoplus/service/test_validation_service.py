import unittest

from fastjsonschema import JsonSchemaException

from dynamoplus.service.validation_service import is_collection_schema_valid, validate_client_authorization_api_key,\
    __validate as validate, validate_query
from dynamoplus.dynamo_plus_v2 import validate_collection, validate_index,  validate_client_authorization,\
    validate_aggregation_configuration


class TestValidationService(unittest.TestCase):

    def test_invalid_schema(self):
        collection_schema = {"type": "notexistingtype"}
        result = is_collection_schema_valid(collection_schema)
        self.assertFalse(result)

    def test_valid_schema(self):
        collection_schema = {"type": "string"}
        result = is_collection_schema_valid(collection_schema)
        self.assertTrue(result)

    def test_valid_schema_complex(self):
        collection_schema = {
            "type": "object",
            "properties": {
                "firstName": {
                    "type": "string",
                    "description": "The person's first name."
                },
                "lastName": {
                    "type": "string",
                    "description": "The person's last name."
                },
                "age": {
                    "description": "Age in years which must be equal to or greater than zero.",
                    "type": "integer",
                    "minimum": 0
                }
            }}
        result = is_collection_schema_valid(collection_schema)
        self.assertTrue(result)

    def test_validate_person_schema_success(self):
        collection_schema = {
            "type": "object",
            "properties": {
                "firstName": {
                    "type": "string",
                    "description": "The person's first name."
                },
                "lastName": {
                    "type": "string",
                    "description": "The person's last name."
                },
                "age": {
                    "description": "Age in years which must be equal to or greater than zero.",
                    "type": "integer",
                    "minimum": 0
                }
            }}
        validate({"firstName": "Ambrogio", "lastName": "Fumagalli", "age": 20},
                          collection_schema)
        ##no error

    def test_validate_person_schema_error(self):
        collection_schema = {
            "type": "object",
            "properties": {
                "firstName": {
                    "type": "string",
                    "description": "The person's first name."
                },
                "lastName": {
                    "type": "string",
                    "description": "The person's last name."
                },
                "age": {
                    "description": "Age in years which must be equal to or greater than zero.",
                    "type": "integer",
                    "minimum": 0
                }
            }}
        self.assertRaises(JsonSchemaException, validate,
                          {"firstName": "Ambrogio", "lastName": "Fumagalli", "age": "twenty"}, collection_schema)

    def test_validate_collection_success(self):
        collection = {
            "id_key": "x",
            "name": "y",
            "ordering": "z",
        }
        validate_collection(collection)
        collection = {
            "id_key": "x",
            "name": "y"
        }
        validate_collection(collection)

    def test_validate_collection_complex_success(self):
        collection = {
            "id_key": "x",
            "name": "y",
            "ordering": "z",
            "attributes":[
                {
                    "type":"STRING",
                    "name": "x"
                },
                {
                    "type": "NUMBER",
                    "name": "y",
                    "constraint": "NOT_NULL"
                },
                {
                    "type": "OBJECT",
                    "name": "z",
                    "attributes":[
                        {"type":"STRING","name": "az"},
                        {"type": "STRING", "name": "bz"}
                    ]
                },
                {
                    "type":"ARRAY",
                    "name": "w",
                    "attributes":[
                        {"type": "STRING", "name": "aw"},
                        {"type": "STRING", "name": "bw"}
                    ]
                }
            ]
        }
        validate_collection(collection)
        collection = {
            "id_key": "x",
            "name": "y"
        }
        validate_collection(collection)

    def test_validate_collection_error(self):
        collection = {
            "id_key": "x",
            "ordering": "z"
        }
        self.assertRaises(JsonSchemaException, validate_collection, collection)

        collection = {
            "name": "y"
        }
        self.assertRaises(JsonSchemaException, validate_collection, collection)

    def test_validate_index_success(self):
        index = {
            "name": "index_name",
            "collection": {
                "id_key": "x",
                "name": "y",
                "ordering": "z"
            },
            "conditions":[
                "c.1","c.2","c.3"
            ]
        }
        validate_index(index)

    def test_validate_index_error(self):
        index = {
            "name": "index_name",
            "collection": {
                "id_key": "x",
                "name": "y",
                "ordering": "z"
            }
        }

        self.assertRaises(JsonSchemaException, validate_index, index)
        index = {
            "name": "index_name",
            "conditions": [
                "c.1", "c.2", "c.3"
            ]
        }
        self.assertRaises(JsonSchemaException, validate_index, index)
        index = {
            "collection": {
                "id_key": "x",
                "name": "y",
                "ordering": "z"
            }
        }
        self.assertRaises(JsonSchemaException, validate_index, index)

    def test_validate_client_authorization_api_key_success(self):
        client_authorization={
            "client_id": "a",
            "type":"api_key",
            "client_scopes":[
                {"collection_name":"a","scope_type":"GET"}
            ],
            "api_key": "X"
        }
        validate_client_authorization_api_key(client_authorization)
        validate_client_authorization(client_authorization)

    def test_validate_client_authorization_api_key_error(self):
        client_authorization = {
            "type": "api_key",
            "client_scopes": [
                {"collection_name": "a", "scope_type": "GET"}
            ],
            "api_key": "X"
        }
        self.assertRaises(JsonSchemaException, validate_client_authorization_api_key, client_authorization)
        self.assertRaises(JsonSchemaException, validate_client_authorization, client_authorization)
        client_authorization = {
            "client_id": "a",
            "type": "api_key",
            "client_scopes": [
                {"collection_name": "a", "scope_type": "GET"}
            ]
        }
        self.assertRaises(JsonSchemaException, validate_client_authorization_api_key, client_authorization)
        self.assertRaises(JsonSchemaException, validate_client_authorization, client_authorization)
        client_authorization = {
            "client_id": "a",
            "type": "api_key",
            "client_scopes": [
                {"collection_name": "a", "scope_type": "x"}
            ],
            "api_key": "X"
        }
        self.assertRaises(JsonSchemaException, validate_client_authorization_api_key, client_authorization)
        self.assertRaises(JsonSchemaException, validate_client_authorization, client_authorization)
        client_authorization = {
            "client_id": "a",
            "type": "X",
            "client_scopes": [
                {"collection_name": "a", "scope_type": "GET"}
            ],
            "api_key": "X"
        }
        self.assertRaises(JsonSchemaException, validate_client_authorization_api_key, client_authorization)
        self.assertRaises(JsonSchemaException, validate_client_authorization, client_authorization)

    def test_validate_query_errors(self):
        query = {

        }
        self.assertRaises(JsonSchemaException, validate_query, query)

        query = {
            "matches":{
                "eq":{

                }
            }
        }
        self.assertRaises(JsonSchemaException, validate_query, query)

        query = {
            "matches": {
                "and": {

                }
            }
        }
        self.assertRaises(JsonSchemaException, validate_query, query)

        query = {
            "matches": {
                "and": [
                    {
                        "whatever": "x"
                    }
                ]
            }
        }
        self.assertRaises(JsonSchemaException, validate_query, query)

    def test_validate_query_valid(self):
        query = {
            "matches":{
                "eq":{
                    "field_name": "x",
                    "value": "y"
                }
            }
        }
        validate_query(query)

        query = {
            "matches": {
                "and":[
                    {
                        "eq": {
                            "field_name": "x",
                            "value": "y"
                        }
                    },
                    {
                        "eq": {
                            "field_name": "Z",
                            "value": "1"
                        }
                    }
                ]

            }
        }
        validate_query(query)

    def test_validate_aggregation_valid(self):
        aggregation = {
            "collection": {
                "name": "example"
            },
            "type": "AVG",
            "configuration": {
                "on": ["INSERT"],
                "target_field": "x",
                "join": {
                    "collection_name": "restaurant",
                    "using_field": "restaurant_id"
                }
            }
        }
        validate_aggregation_configuration(aggregation)

    def test_validate_aggregation_errors(self):
        aggregation = {

        }
        self.assertRaises(JsonSchemaException, validate_aggregation_configuration, aggregation)
        aggregation = {
            "collection": {

            },
            "type": "AVG_JOIN",
            "aggregation":{
                "on":["INSERT"],
                "target_field": "x",
                "join": {
                    "collection_name": "restaurant",
                    "using_field": "restaurant_id"
                }
            }
        }
        self.assertRaises(JsonSchemaException, validate_aggregation_configuration, aggregation)
        aggregation = {
            "collection": {
                "name": "example"
            },
            "aggregation": {
                "on": ["INSERT"],
                "target_field": "x",
                "join": {
                    "collection_name": "restaurant",
                    "using_field": "restaurant_id"
                }
            }
        }
        self.assertRaises(JsonSchemaException, validate_aggregation_configuration, aggregation)
        aggregation = {
            "collection": {
                "name": "example"
            },
            "type": "AVG_JOIN"
        }
        self.assertRaises(JsonSchemaException, validate_aggregation_configuration, aggregation)
        aggregation = {
            "collection": {
                "name": "example"
            },
            "type": "AVG_JOIN",
            "aggregation": {
                "target_field": "x",
                "join": {
                    "collection_name": "restaurant",
                    "using_field": "restaurant_id"
                }
            }
        }
        self.assertRaises(JsonSchemaException, validate_aggregation_configuration, aggregation)