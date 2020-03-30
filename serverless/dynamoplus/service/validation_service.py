import fastjsonschema
import logging
from fastjsonschema import JsonSchemaDefinitionException, JsonSchemaException

logger = logging.getLogger()
logger.setLevel(logging.INFO)

COLLECTION_ATTRIBUTE_BASE_SCHEMA_DEFINITION = {
    "name": {"type": "string"},
    "type": {"type": "string", "enum": ["STRING", "OBJECT", "NUMBER", "DATE", "ARRAY"]},
    "constraints": {"type": "string", "enum": ["NULLABLE", "NOT_NULL"]}
}

COLLECTION_ATTRIBUTE_SCHEMA_DEFINITION = {
    "properties": {
        **COLLECTION_ATTRIBUTE_BASE_SCHEMA_DEFINITION,
        "attributes": {
            "type": "array",
            "items": { "properties": COLLECTION_ATTRIBUTE_BASE_SCHEMA_DEFINITION }
        }
    },
    "required":["name","type"]
}
COLLECTION_SCHEMA_DEFINITION = {
    "properties": {
        "id_key": {"type": "string"},
        "name": {"type": "string"},
        "ordering": {"type": "string"},
        "attributes": {
            "type": "array",
            "items": COLLECTION_ATTRIBUTE_SCHEMA_DEFINITION
        }
    },
    "required": [
        "id_key",
        "name"
    ]
}

INDEX_SCHEMA_DEFINITION = {
    "properties": {
        "uid": {"type": "string", "pattern": "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"},
        "name": {"type": "string"},
        "collection": COLLECTION_SCHEMA_DEFINITION,
        "conditions": {"type": "array", "items": {"type": "string"}}
    },
    "required": [
        "uid",
        "name",
        "collection",
        "conditions"
    ]
}

CLIENT_SCOPE_SCHEMA_DEFINITION = {
    "properties": {
        "collection_name": {"type": "string"},
        "scope_types": {"type": "array",
                        "items": {"type": "string", "enum": ["CREATE", "UPDATE", "GET", "DELETE", "QUERY"]}}
    },
    "required": ["collection_name", "scope_types"]
}
CLIENT_AUTHORIZATION_SCHEMA_DEFINITION = {
    "properties": {
        "client_id": {"type": "string"},
        "client_scopes": {"type": "array", "items": CLIENT_SCOPE_SCHEMA_DEFINITION},
        "type": {"type": "string", "enum": ["http_signature", "api_key"]}
    },
    "required": [
        "client_id",
        "client_scopes"
    ]
}
CLIENT_AUTHORIZATION_HTTP_SIGNATURE_SCHEMA_DEFINITION = {
    "properties": {
        "client_public_key": {"type": "string"}
    },
    "required": ["client_public_key"]
}
CLIENT_AUTHORIZATION_API_KEY_SCHEMA_DEFINITION = {
    "properties": {
        "api_key": {"type": "string"}
    },
    "required": ["api_key"]
}
QUERY_SCHEMA_DEFINITION = {
    "type": "object",
    "properties": {
        "matches": {
            "type": "object",
            "properties": {
                "collection": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "collection name"
                        }

                    },
                    "required": ["name"]
                }
            },
            "required": ["collection"]
        }
    },
    "required": ["matches"]
}


def __validate(d: dict, schema: dict):
    validator = __compile_json_schema(schema)
    if validator:
        validator(d)
    else:
        raise SyntaxError("invalid json schema")


def __compile_json_schema(collection_schema):
    try:
        return fastjsonschema.compile(collection_schema)
    except JsonSchemaDefinitionException as e:
        logger.error("unable to compile schema definition", e)
        return None


def is_collection_schema_valid(collection_schema: dict):
    return __compile_json_schema(collection_schema) is not None


def validate_index(index: dict):
    __validate(index, INDEX_SCHEMA_DEFINITION)


def validate_collection(collection: dict):
    __validate(collection, COLLECTION_SCHEMA_DEFINITION)


def validate_client_authorization_http_signature(client_authorization: dict):
    __validate(client_authorization, CLIENT_AUTHORIZATION_SCHEMA_DEFINITION)
    __validate(client_authorization, CLIENT_AUTHORIZATION_HTTP_SIGNATURE_SCHEMA_DEFINITION)


def validate_client_authorization_api_key(client_authorization: dict):
    __validate(client_authorization, CLIENT_AUTHORIZATION_SCHEMA_DEFINITION)
    __validate(client_authorization, CLIENT_AUTHORIZATION_API_KEY_SCHEMA_DEFINITION)


def validate_document(document: dict, collection_schema: dict):
    __validate(document, collection_schema)
