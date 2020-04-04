import unittest
import uuid

from fastjsonschema import JsonSchemaException

from dynamoplus.service.validation_service import is_collection_schema_valid, validate_document, validate_collection, \
    validate_index, validate_client_authorization_api_key, __validate as validate, validate_client_authorization


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
            "uid": str(uuid.uuid4()),
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
            "uid": str(uuid.uuid4()),
            "name": "index_name",
            "collection": {
                "id_key": "x",
                "name": "y",
                "ordering": "z"
            }
        }
        self.assertRaises(JsonSchemaException, validate_index, index)
        index = {
            "uid": str(uuid.uuid4()),
            "name": "index_name",
            "conditions": [
                "c.1", "c.2", "c.3"
            ]
        }
        self.assertRaises(JsonSchemaException, validate_index, index)
        index = {
            "uid": str(uuid.uuid4()),
            "collection": {
                "id_key": "x",
                "name": "y",
                "ordering": "z"
            },
            "conditions": [
                "c.1", "c.2", "c.3"
            ]
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
