import fastjsonschema
from fastjsonschema import JsonSchemaDefinitionException, JsonSchemaException


class ValidationService:

    @staticmethod
    def is_collection_schema_valid(collection_schema: dict):
        return ValidationService.__compile_json_schema(collection_schema) is not None

    @staticmethod
    def validate_document(document: dict, collection_schema: dict):
        validator = ValidationService.__compile_json_schema(collection_schema)
        if validator:
            validator(document)
        else:
            raise SyntaxError("invalid json schema")

    @staticmethod
    def __compile_json_schema(collection_schema):
        try:
            return fastjsonschema.compile(collection_schema)
        except JsonSchemaDefinitionException as e:
            return None
