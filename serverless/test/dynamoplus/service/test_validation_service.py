import unittest

from fastjsonschema import JsonSchemaException

from dynamoplus.service.validation_service import ValidationService


class TestValidationService(unittest.TestCase):

    def test_invalid_collection_schema(self):
        collection_schema = {"type": "notexistingtype"}
        result = ValidationService.is_collection_schema_valid(collection_schema)
        self.assertFalse(result)

    def test_valid_collection_schema_simple(self):
        collection_schema = {"type": "string"}
        result = ValidationService.is_collection_schema_valid(collection_schema)
        self.assertTrue(result)

    def test_valid_collection_schema_complex(self):
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
        result = ValidationService.is_collection_schema_valid(collection_schema)
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
        ValidationService.validate_document({"firstName": "Ambrogio", "lastName": "Fumagalli", "age": 20},
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
        self.assertRaises(JsonSchemaException,ValidationService.validate_document, {"firstName": "Ambrogio", "lastName": "Fumagalli", "age": "twenty"},collection_schema)

        ##no error
