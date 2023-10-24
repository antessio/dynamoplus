import fastjsonschema
import logging
from fastjsonschema import JsonSchemaDefinitionException, JsonSchemaException

from dynamoplus.models.system.aggregation.aggregation import AggregationType, AggregationTrigger
from dynamoplus.models.system.collection.collection import AttributeType, AttributeDefinition, AttributeConstraint
from dynamoplus.v2.service.system.system_service_v2 import Collection

logger = logging.getLogger()
logger.setLevel(logging.INFO)

COLLECTION_ATTRIBUTE_BASE_SCHEMA_DEFINITION = {
    "name": {"type": "string"},
    "type": {"type": "string", "enum": ["STRING", "OBJECT", "NUMBER", "DATE", "ARRAY"]},
    "constraints": {"type": "array", "items": {"type": "string", "enum": ["NULLABLE", "NOT_NULL"]}}
}

COLLECTION_ATTRIBUTE_SCHEMA_DEFINITION = {
    "properties": {
        **COLLECTION_ATTRIBUTE_BASE_SCHEMA_DEFINITION,
        "attributes": {
            "type": "array",
            "items": {"properties": COLLECTION_ATTRIBUTE_BASE_SCHEMA_DEFINITION}
        }
    },
    "required": ["name", "type"]
}
COLLECTION_SCHEMA_DEFINITION = {
    "properties": {
        "id_key": {"type": "string"},
        "name": {"type": "string"},
        "ordering": {"type": "string"},
        "auto_generate_id": {"type": "boolean"},
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

BASE_COLLECTION_SCHEMA_DEFINITION = {
    "properties": {
        "name": {"type": "string"}
    },
    "required": ["name"]
}
EQ_PREDICATE_SCHEMA_DEFINITION = {
    "properties": {
        "field_name": {"type": "string"},
        "value": {"type": "string"}
    },
    "required": [
        "field_name",
        "value"
    ]
}
RANGE_PREDICATE_SCHEMA_DEFINITION = {
    "properties": {
        "field_name": {"type": "string"},
        "from": {"type": "string"},
        "to": {"type": "string"}
    },
    "required": ["field_name", "from", "to"]
}
MATCHES_SCHEMA_DEFINITION = {
    "type": "object",
    "oneOf": [
        {
            "required": ["and"]
        },
        {
            "required": ["eq"]
        },
        {
            "required": ["range"]
        }
    ],
    "properties": {
        "eq": EQ_PREDICATE_SCHEMA_DEFINITION,
        "and": {
            "type": "array",
            "items": {
                "oneOf":[
                    {
                        "required": ["eq"]
                    },
                    {
                        "required": ["range"]
                    }
                ],
                "properties": {
                    "eq": EQ_PREDICATE_SCHEMA_DEFINITION
                }
            }
        },
        "range": RANGE_PREDICATE_SCHEMA_DEFINITION
    }
}
AGGREGATION_CONFIGURATION_SCHEMA_DEFINITION = {
    "properties": {
        "collection": BASE_COLLECTION_SCHEMA_DEFINITION,
        "type": {
            "type": "string", "enum": AggregationType.types()
        },
        "configuration": {
            "properties": {
                "on": {"type": "array", "items": {"type": "string", "enum": AggregationTrigger.types()}},
                "target_field": {"type": "string"},
                "matches": MATCHES_SCHEMA_DEFINITION
            },
            "required": ["on"]
        }
    },
    "required": [
        "collection", "type", "configuration"
    ]
}

INDEX_SCHEMA_DEFINITION = {
    "properties": {
        "collection": BASE_COLLECTION_SCHEMA_DEFINITION,
        "conditions": {"type": "array", "items": {"type": "string"}},
        "configuration": {"type": "string", "enum": ["OPTIMIZE_READ", "OPTIMIZE_WRITE"]}
    },
    "required": [
        "collection",
        "conditions"
    ]
}

CLIENT_SCOPE_SCHEMA_DEFINITION = {
    "properties": {
        "collection_name": {"type": "string"},
        "scope_type": {"type": "string", "enum": ["CREATE", "UPDATE", "GET", "DELETE", "QUERY"]}
    },
    "required": ["collection_name", "scope_type"]
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
        "public_key": {"type": "string"}
    },
    "required": ["public_key"]
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
        "matches": MATCHES_SCHEMA_DEFINITION
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
        logger.error("unable to compile schema definition {}".format(e))
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


def validate_query(query: dict):
    __validate(query, QUERY_SCHEMA_DEFINITION)


def validate_aggregation_configuration(aggregation_configuration: dict):
    __validate(aggregation_configuration, AGGREGATION_CONFIGURATION_SCHEMA_DEFINITION)


def validate_document(document: dict, collection_metadata: Collection):
    required_attributes = []
    if not collection_metadata.auto_generated_id:
        required_attributes.append(collection_metadata.id_key)
    properties_schema = {
        collection_metadata.id_key: {"type": "string"}
    }
    if collection_metadata.attributes:
        attributes_schema = dict(map(from_attribute_definition_to_schema, collection_metadata.attributes))
        collection_required_attributes = list(map(lambda a: a.name, filter(
            lambda a: a.constraints is not None and AttributeConstraint.NOT_NULL in a.constraints,
            collection_metadata.attributes)))
        required_attributes.extend(collection_required_attributes)
        properties_schema.update(attributes_schema)
    document_schema = {
        "type": "object",
        "properties": properties_schema,
        "required": required_attributes
    }
    __validate(document, document_schema)


def validate_client_authorization(client_authorization: dict):
    t = client_authorization["type"]
    validators_map = {
        "api_key": validate_client_authorization_api_key,
        "http_signature": validate_client_authorization_http_signature
    }
    if t in validators_map:
        validators_map[t](client_authorization)
    else:
        raise JsonSchemaException("type not valid")


# def validate_aggregation(aggregation: dict):
#     __validate(aggregation, AGGREGATION_CONFIGURATION_SCHEMA_DEFINITION)


# def validate_document(document: dict, collection_schema: dict):
#    __validate(document, collection_schema)


def from_attribute_type_to_schema(type: AttributeType):
    return {
        AttributeType.STRING: "string",
        AttributeType.NUMBER: "number",
        AttributeType.OBJECT: "object",
        AttributeType.ARRAY: "array",
        AttributeType.DATE: "string",
        AttributeType.BOOLEAN: "boolean"
    }[type]


def from_attribute_definition_to_schema(a: AttributeDefinition):
    d = {"type": from_attribute_type_to_schema(a.type)}
    if a.attributes:
        nested_attributes = dict(map(from_attribute_definition_to_schema, a.attributes))
        d["attributes"] = nested_attributes
    return a.name, d
