import unittest

from fastjsonschema import JsonSchemaException

from dynamoplus.service.validation_service import is_collection_schema_valid, validate_document, validate_collection


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
        validate_document({"firstName": "Ambrogio", "lastName": "Fumagalli", "age": 20},
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
        self.assertRaises(JsonSchemaException, validate_document, {"firstName": "Ambrogio", "lastName": "Fumagalli", "age": "twenty"}, collection_schema)

    def test_validate_collection_success(self):
        collection = {
            "id_key": "x",
            "name": "y",
            "ordering": "z"
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