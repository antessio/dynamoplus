import json
import logging
from apispec import APISpec

from aws.http.info import VERSION
from dynamoplus.models.system.index.index import Index
from dynamoplus.service.system.system import SystemService
from dynamoplus.service.validation_service import COLLECTION_SCHEMA_DEFINITION, INDEX_SCHEMA_DEFINITION, \
    CLIENT_AUTHORIZATION_SCHEMA_DEFINITION, CLIENT_AUTHORIZATION_HTTP_SIGNATURE_SCHEMA_DEFINITION, \
    CLIENT_AUTHORIZATION_API_KEY_SCHEMA_DEFINITION, QUERY_SCHEMA_DEFINITION, CLIENT_SCOPE_SCHEMA_DEFINITION

from dynamoplus.utils.utils import get_schema_from_conditions

logging.basicConfig(level=logging.INFO)


def swagger_json(event, context):
    query_parameters = event['queryStringParameters']
    target_collection_name = query_parameters[
        "collection_name"] if query_parameters and "collection_name" in query_parameters else None
    api_description = "DynamoPlus API"
    if target_collection_name:
        api_description = api_description + " - collection name: {}".format(target_collection_name)
    else:
        api_description = api_description + " - system collections"
    spec = APISpec(
        title="DynamoPlus",
        version=VERSION,
        openapi_version="3.0.2",
        info=dict(description=api_description),
    )
    if target_collection_name is None:
        spec.components.schema(
            "Collection",
            COLLECTION_SCHEMA_DEFINITION
        )
        spec.path(path="/dynamoplus/collection/{collection_name}", operations={
            'get': {
                'tags': ['collection'],
                'parameters': [{
                    "name": "collection_name",
                    "in": "path",
                    "description": "collection name",
                    "required": True,
                    "schema": {"type": "string"}
                }],
                'description': "get collection by name",
                'responses': {
                    "200": {"description": "collection object found",
                            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Collection"}}}},
                    "404": {"description": "collection object found", "content": {}}
                }},
            'delete': {
                'tags': ['collection'],
                'parameters': [{
                    "name": "collection_name",
                    "in": "path",
                    "description": "collection name",
                    "required": True,
                    "schema": {"type": "string"}
                }],
                'description': "get collection by name",
                'responses': {
                    "200": {"description": "collection deleted"},
                    "404": {"description": "collection object not found", "content": {}},
                    "403": {"description": "Access forbidden for system API"}
                }}})
        spec.path(path="/dynamoplus/collection", operations={
            'get': {
                'tags': ['collection'],
                'description': "get all collections",
                'parameters': [
                    {
                        "name": "limit",
                        "in": "query",
                        "description": "number of elements",
                        "required": False,
                        "schema": {"type": "integer"}
                    },
                    {
                        "name": "last_key",
                        "in": "query",
                        "description": "last id",
                        "required": False,
                        "schema": {"type": "string"}
                    }
                ],
                'responses': {
                    "200": {"description": "collections list",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "data": {
                                                "type": "array",
                                                "items": {"$ref": "#/components/schemas/Collection"}
                                            },
                                            "has_more": {"type": "boolean"}
                                        }}}}},
                    "403": {"description": "Access forbidden for system API"}
                }
            },
            'post': {
                'tags': ['collection'],
                'description': "create a new collection",
                'requestBody': {
                    "required": True,
                    "content": {
                        "application/json": {"schema": {
                            "$ref": "#/components/schemas/Collection"}}
                    }
                },
                'responses': {
                    "201": {"description": "collection created", "content": {
                        "application/json": {"schema": {"$ref": "#/components/schemas/Collection"}}}},
                    "403": {"description": "Access forbidden for system API"}
                }}})
        spec.path(path="/dynamoplus/collection/query", operations={
            'post': {
                'tags': ['collection'],
                'description': "get all collections",
                'responses': {
                    "200": {"description": "collections list", "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "data": {
                                        "type": "array",
                                        "items": {"$ref": "#/components/schemas/Collection"}
                                    },
                                    "lastKey": {"type": "string"}
                                }}}}},
                    "403": {"description": "Access forbidden for system API"}
                }
            }
        })

        spec.components.schema(
            "Index",
            INDEX_SCHEMA_DEFINITION
        )
        spec.path(path="/dynamoplus/index/{index_id}", operations={
            'get': {
                'tags': ['index'],
                'description': "get index by id",
                'parameters': [{
                    "name": "index_id",
                    "in": "path",
                    "description": "index id",
                    "required": True,
                    "schema": {"type": "string"}
                }],
                'responses': {
                    "200": {
                        "description": "index found",
                        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Index"}}}
                    },
                    "404": {"description": "index object found", "content": {}},
                    "403": {"description": "Access forbidden for system API"}
                }
            },
            'delete': {
                'tags': ['index'],
                'description': "delete an index by id",
                'parameters': [{
                    "name": "index_id",
                    "in": "path",
                    "description": "index id",
                    "required": True,
                    "schema": {"type": "string"}
                }],
                'responses': {
                    "200": {
                        "description": "index deleted"
                    },
                    "404": {"description": "index object found", "content": {}},
                    "403": {"description": "Access forbidden for system API"}
                }
            }
        })
        spec.path(path="/dynamoplus/index", operations={
            'get': {
                'tags': ['index'],
                'description': "get all indexes",
                'parameters': [
                    {
                        "name": "limit",
                        "in": "query",
                        "description": "number of elements",
                        "required": False,
                        "schema": {"type": "integer"}
                    },
                    {
                        "name": "last_key",
                        "in": "query",
                        "description": "last id",
                        "required": False,
                        "schema": {"type": "string"}
                    }
                ],
                'responses': {
                    "200": {"description": "index list",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "data": {
                                                "type": "array",
                                                "items": {"$ref": "#/components/schemas/Index"}
                                            },
                                            "has_more": {"type": "boolean"}
                                        }}}}},
                    "403": {"description": "Access forbidden for system API"}
                }
            },
            'post': {
                'tags': ['index'],
                'description': "create a new index",
                'requestBody': {
                    "required": True,
                    "content": {
                        "application/json": {"schema": {"$ref": "#/components/schemas/Index"}}
                    }
                },
                'responses': {
                    "201": {
                        "description": "index created",
                        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Index"}}}
                    },
                    "403": {"description": "Access forbidden for system API"}
                }
            }
        })
        spec.path(path="/dynamoplus/index/query", operations={
            'post': {
                'tags': ['index'],
                'description': "query index by collection",
                'requestBody': {
                    "required": True,
                    "content": {
                        "application/json": {"schema": QUERY_SCHEMA_DEFINITION}
                    }
                },
                'responses': {
                    "200": {"description": "index list", "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "data": {
                                        "type": "array",
                                        "items": {"$ref": "#/components/schemas/Index"}
                                    },
                                    "lastKey": {"type": "string"}
                                }}}}},
                    "403": {"description": "Access forbidden for system API"}
                }
            }
        })
        spec.components.schema(
            "ClientScope",
            CLIENT_SCOPE_SCHEMA_DEFINITION)
        spec.components.schema(
            "ClientAuthorization",
            CLIENT_AUTHORIZATION_SCHEMA_DEFINITION
        )
        spec.components.schema(
            "ClientAuthorizationHttpSignature",
            {
                "allOf": [
                    {"$ref": "#/components/schemas/ClientAuthorization"},
                    CLIENT_AUTHORIZATION_HTTP_SIGNATURE_SCHEMA_DEFINITION
                ]
            }
        )
        spec.components.schema(
            "ClientAuthorizationApiKey",
            {
                "allOf": [
                    {"$ref": "#/components/schemas/ClientAuthorization"},
                    CLIENT_AUTHORIZATION_API_KEY_SCHEMA_DEFINITION
                ]
            }
        )
        spec.path(path="/dynamoplus/client_authorization/{client_authorization_id}", operations={
            'get': {
                'tags': ['client_authorization'],
                'description': "get index by id",
                'parameters': [{
                    "name": "client_authorization_id",
                    "in": "path",
                    "description": "client authorization id",
                    "required": True,
                    "schema": {"type": "string"}
                }],
                'responses': {
                    "200": {
                        "description": "client authorization found",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/ClientAuthorization"}}}
                    },
                    "404": {"description": "client authorization object found", "content": {}},
                    "403": {"description": "Access forbidden for system API"}
                }
            },
            'delete': {
                'tags': ['client_authorization'],
                'description': "delete a client_authorization by id",
                'parameters': [{
                    "name": "client_authorization_id",
                    "in": "path",
                    "description": "client authorization id",
                    "required": True,
                    "schema": {"type": "string"}
                }],
                'responses': {
                    "200": {
                        "description": "client authorization deleted"
                    },
                    "404": {"description": "client authorization object found", "content": {}},
                    "403": {"description": "Access forbidden for system API"}
                }
            }
        })
        spec.path(path="/dynamoplus/client_authorization", operations={
            'get': {
                'tags': ['client_authorization'],
                'description': "get all client authorizations",
                'parameters': [
                    {
                        "name": "limit",
                        "in": "query",
                        "description": "number of elements",
                        "required": False,
                        "schema": {"type": "integer"}
                    },
                    {
                        "name": "last_key",
                        "in": "query",
                        "description": "last id",
                        "required": False,
                        "schema": {"type": "string"}
                    }
                ],
                'responses': {
                    "200": {"description": "client authorization list",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "data": {
                                                "type": "array",
                                                "items": {
                                                    "oneOf": [
                                                        {"$ref": "#/components/schemas/ClientAuthorizationApiKey"},
                                                        {"$ref": "#/components/schemas/ClientAuthorizationHttpSignature"}
                                                    ]
                                                }
                                            },
                                            "has_more": {"type": "boolean"}
                                        }}}}},
                    "403": {"description": "Access forbidden for system API"}
                }
            },
            'post': {
                'tags': ['client_authorization'],
                'description': "create a new client_authorization",
                'requestBody': {
                    "required": True,
                    "content": {
                        "application/json": {"schema": {
                            "oneOf": [{"$ref": "#/components/schemas/ClientAuthorizationApiKey"},
                                      {"$ref": "#/components/schemas/ClientAuthorizationHttpSignature"}]}}
                    }
                },
                'responses': {
                    "201": {
                        "description": "client_authorization created",
                        "content": {"application/json": {"schema": {
                            "oneOf": [{"$ref": "#/components/schemas/ClientAuthorizationApiKey"},
                                      {"$ref": "#/components/schemas/ClientAuthorizationHttpSignature"}]}}}
                    },
                    "403": {"description": "Access forbidden for system API"}
                }
            }
        })
        spec.path(path="/dynamoplus/client_authorization", operations={
            'put': {
                'tags': ['client_authorization'],
                'description': "update client_authorization",
                'requestBody': {
                    "required": True,
                    "content": {
                        "application/json": {"schema": {
                            "oneOf": [{"$ref": "#/components/schemas/ClientAuthorizationApiKey"},
                                      {"$ref": "#/components/schemas/ClientAuthorizationHttpSignature"}
                                      ]}}
                    }
                },
                'responses': {
                    "200": {
                        "description": "client_authorization updated",
                        "content": {"application/json": {"schema": {
                            "oneOf": [
                                {"$ref": "#/components/schemas/ClientAuthorizationApiKey"},
                                {"$ref": "#/components/schemas/ClientAuthorizationHttpSignature"}
                            ]}}}
                    },
                    "403": {"description": "Access forbidden for system API"}
                }
            }
        })
    else:
        c = SystemService.get_collection_by_name(target_collection_name)
        add_collection(c, spec)
    # last_evaluted_key = None
    # collections, last_evaluted_key = SystemService.get_all_collections(10, last_evaluted_key)
    # logging.info("adding collections to spec (last_evaluated_key={})".format(last_evaluted_key))
    # add_collections_to_spec(collections, spec)
    # while last_evaluted_key:
    #     collections, last_evaluted_key = SystemService.get_all_collections(10, last_evaluted_key)
    #     add_collections_to_spec(collections, spec)
    return {"statusCode": 200, "body": json.dumps(spec.to_dict())}


def add_collection(c, spec):
    # properties = {attr.attribute_name: {"type": attr.attribute_type} for attr in
    #               c.attribute_definition} if c.attribute_definition else {}
    properties = {c.id_key: {"type": "string"}}
    required = [c.id_key]
    if c.ordering_key:
        properties[c.ordering_key] = {"type": "string"}
        required.append(c.ordering_key)
    spec.components.schema(
        c.name,
        {
            "properties": properties,
            "required": required
        }
    )
    spec.path(path="/dynamoplus/{}/{{{}_id}}".format(c.name, c.name), operations={
        'get': {
            'tags': [c.name],
            'description': "get {} by id".format(c.name),
            'parameters': [{
                "name": "{}_id".format(c.name),
                "in": "path",
                "description": "{} id".format(c.name),
                "required": True,
                "schema": {"type": "string"}
            }],
            'responses': {
                "200": {
                    "description": "{} found".format(c.name),
                    "content": {"application/json": {
                        "schema": {"$ref": "#/components/schemas/{}".format(c.name)}}}
                },
                "404": {"description": "{} object found".format(c.name)},
                "403": {"description": "Access forbidden "}
            }
        },
        'delete': {
            'tags': [c.name],
            'description': "delete a {} by id".format(c.name),
            'parameters': [{
                "name": "{}_id".format(c.name),
                "in": "path",
                "description": "{} id".format(c.name),
                "required": True,
                "schema": {"type": "string"}
            }],
            'responses': {
                "200": {
                    "description": "{} deleted".format(c.name)
                },
                "404": {"description": "{} object found".format(c.name)},
                "403": {"description": "Access forbidden"}
            }
        }
    })
    spec.path(path="/dynamoplus/{}".format(c.name), operations={
        'get': {
            'tags': [c.name],
            'description': "get all {}".format(c.name),
            'parameters': [
                {
                    "name": "limit",
                    "in": "query",
                    "description": "number of elements",
                    "required": False,
                    "schema": {"type": "integer"}
                },
                {
                    "name": "last_key",
                    "in": "query",
                    "description": "last id",
                    "required": False,
                    "schema": {"type": "string"}
                }
            ],
            'responses': {
                "200": {"description": "{} list".format(c.name),
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {
                                            "type": "array",
                                            "items": {"$ref": "#/components/schemas/{}".format(c.name)}
                                        },
                                        "has_more": {"type": "boolean"}
                                    }}}}},
                "403": {"description": "Access forbidden for system API"}
            }
        },
        'post': {
            'tags': [c.name],
            'description': "create a new {}".format(c.name),
            'requestBody': {
                "required": True,
                "content": {
                    "application/json": {"schema": {"$ref": "#/components/schemas/{}".format(c.name)}}
                }
            },
            'responses': {
                "201": {
                    "description": "index created",
                    "content": {"application/json": {"schema": {"$ref": "#/components/schemas/{}".format(c.name)}}}
                },
                "403": {"description": "Access forbidden for system API"}
            }
        }
    })
    spec.path(path="/dynamoplus/{}".format(c.name), operations={
        'put': {
            'tags': [c.name],
            'description': "update {}".format(c.name),
            'requestBody': {
                "required": True,
                "content": {
                    "application/json": {"schema": {"$ref": "#/components/schemas/{}".format(c.name)}}
                }
            },
            'responses': {
                "200": {
                    "description": "{} updated".format(c.name),
                    "content": {"application/json": {"schema": {"$ref": "#/components/schemas/{}".format(c.name)}}}
                },
                "403": {"description": "Access forbidden"}
            }
        }
    })
    for i in SystemService.get_indexes_from_collection_name_generator(c.name):
        add_query_to_spec(i, spec)


def add_query_to_spec(i: Index, spec):
    formatted_fields = " & ".join(["{}"] * len(i.conditions)).format(*i.conditions)
    description = "get all {} by {}".format(i.collection_name, formatted_fields)
    spec.path(path="/dynamoplus/collection/query/{}".format(i.index_name), operations={
        'post': {
            'tags': [i.collection_name],
            'description': description,
            'requestBody': {
                "required": True,
                "content": {
                    "application/json": {"schema": {
                        "type": "object",
                        "properties": {
                            "matches": {
                                "type": "object",
                                "properties": get_schema_from_conditions(i.conditions)

                            }
                        },
                        "required": ["matches"]
                    }}
                }
            },
            'responses': {
                "200": {
                    "description": "{} list".format(i.collection_name),
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "data": {
                                        "type": "array",
                                        "items": {"$ref": "#/components/schemas/{}".format(i.collection_name)}
                                    },
                                    "lastKey": {"type": "string"}
                                }}}}},
                "403": {"description": "Access forbidden for system API"}
            }
        }})
