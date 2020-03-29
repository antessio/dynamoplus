import fastjsonschema
from fastjsonschema import JsonSchemaDefinitionException, JsonSchemaException

COLLECTION_SCHEMA_DEFINITION = {
    "properties": {
        "id_key": {"type": "string"},
        "name": {"type": "string"},
        "ordering": {"type": "string"}
    },
    "required": [
        "id_key",
        "name"
    ]
}

INDEX_SCHEMA_DEFINITION = {
    "properties": {
        "uid": {"type": "string", "format": "uuid"},
        "name": {"type": "string"},
        "collection": {"$ref": "#/components/schemas/Collection"},
        "conditions": {"type": "array", "items": {"type": "string"}}
    },
    "required": [
        "uid",
        "name",
        "collection",
        "conditions"
    ]
}
CLIENT_AUTHORIZATION_SCHEMA_DEFINITION = {
    "properties": {
        "client_id": {"type": "string"},
        "client_scopes": {"type": "array", "items": {"$ref": "#/components/schemas/ClientScope"}},
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
        return None


def is_collection_schema_valid(collection_schema: dict):
    return __compile_json_schema(collection_schema) is not None


def validate_index(index: dict):
    __validate(index, INDEX_SCHEMA_DEFINITION)

def validate_query(query:dict):
    __validate(query,QUERY_SCHEMA_DEFINITION)

def validate_collection(collection: dict):
    __validate(collection, COLLECTION_SCHEMA_DEFINITION)


def validate_document(document: dict, collection_schema: dict):
    __validate(document, collection_schema)
