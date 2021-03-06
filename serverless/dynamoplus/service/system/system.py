import logging
from typing import *

from dynamoplus.models.query.conditions import Predicate, And, Range, Eq, AnyMatch
from dynamoplus.repository.models import Query as QueryRepository
from dynamoplus.repository.repositories import DynamoPlusRepository, IndexDynamoPlusRepository
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType, \
    AttributeConstraint
from dynamoplus.models.system.index.index import Index
from dynamoplus.models.system.client_authorization.client_authorization import ClientAuthorization, \
    ClientAuthorizationApiKey, ClientAuthorizationHttpSignature, Scope, ScopesType

collectionMetadata = Collection("collection", "name")
index_metadata = Collection("index", "uid")
client_authorization_metadata = Collection("client_authorization", "client_id")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def from_collection_to_dict(collection_metadata: Collection):
    result = {"name": collection_metadata.name, "id_key": collection_metadata.id_key}
    if collection_metadata.ordering_key:
        result["ordering_key"] = collection_metadata.ordering_key
    return result


def from_index_to_dict(index_metadata: Index):
    return {"name": index_metadata.index_name, "collection": {"name": index_metadata.collection_name},
            "conditions": index_metadata.conditions}


def from_dict_to_index(d: dict):
    return Index(d["uid"], d["collection"]["name"], d["conditions"], d["ordering_key"] if "ordering_key" in d else None,
                 d["name"] if "name" in d else None)


# def from_dict_to_client_authorization(d: dict):
#     client_id = d["client_id"]
#     scopes = list(map(lambda s: Scope(s["collection_name"], ScopesType(s["scope_type"])), d["scopes"]))
#     if d["type"].lower() == 'api-key':
#         api_key = d["api_key"]
#         whitelist_hosts = d["whitelist_hosts"] if "whitelist_hosts" in d else None
#         return ClientAuthorizationApiKey(client_id, scopes, api_key, whitelist_hosts)
#     elif d["type"] == 'http-signature':
#         return ClientAuthorizationHttpSignature(client_id, scopes, d["public_key"])
#     else:
#         raise NotImplementedError

def from_client_authorization_http_signature_to_dict(client_authorization: ClientAuthorizationHttpSignature):
    return {
        "type": "http_signature",
        "client_id": client_authorization.client_id,
        "client_scopes": list(map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                                  client_authorization.client_scopes)),
        "public_key": client_authorization.client_public_key
    }


def from_client_authorization_api_key_to_dict(client_authorization: ClientAuthorizationApiKey):
    result = {
        "type": "api_key",
        "client_id": client_authorization.client_id,
        "client_scopes": list(map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                                  client_authorization.client_scopes)),
        "api_key": client_authorization.api_key,
    }
    if client_authorization.whitelist_hosts:
        result["whitelist_hosts"] = client_authorization.whitelist_hosts
    return result


#     if client_authorization.whitelist_hosts:
#         result["whitelist_hosts"] = client_authorization.whitelist_hosts
#     return result


def from_index_to_dict(index_metadata: Index):
    return {
        "uid": index_metadata.uid,
        "name": index_metadata.index_name,
        "collection": {
            "name": index_metadata.collection_name
        },
        "ordering_key": index_metadata._ordering_key,
        "conditions": index_metadata.conditions

    }


def from_collection_to_dict(collection: Collection):
    d = {
        "name": collection.name,
        "id_key": collection.id_key,
        "ordering": collection.ordering_key,
        "auto_generate_id": collection.auto_generate_id
    }
    if collection.attribute_definition:
        attributes = list(map(lambda a: from_attribute_definition_to_dict(a), collection.attribute_definition))
        d["attributes"] = attributes
    return d


def from_dict_to_collection(d: dict):
    attributes = list(map(from_dict_to_attribute_definition, d["attributes"])) if "attributes" in d else None
    auto_generate_id = d["auto_generate_id"] if "auto_generate_id" in d else False
    return Collection(d["name"], d["id_key"], d["ordering"] if "ordering" in d else None, attributes, auto_generate_id)


def from_attribute_definition_to_dict(attribute: AttributeDefinition):
    nested_attributes = list(
        map(lambda a: from_attribute_definition_to_dict(a), attribute.attributes)) if attribute.attributes else None
    return {"name": attribute.name, "type": attribute.type.value, "attributes": nested_attributes}


def from_dict_to_attribute_definition(d: dict):
    attributes = None
    if "attributes" in d and d["attributes"] is not None:
        attributes = list(map(from_dict_to_attribute_definition, d["attributes"]))
    return AttributeDefinition(d["name"], from_string_to_attribute_type(d["type"]),
                               from_array_to_constraints_list(d["constraints"]) if "constraints" in d else None,
                               attributes)


def from_array_to_constraints_list(constraints: List[dict]):
    attribute_constraint_map = {
        "NULLABLE": AttributeConstraint.NULLABLE,
        "NOT_NULL": AttributeConstraint.NOT_NULL
    }
    return list(
        map(lambda c: attribute_constraint_map[c] if c in attribute_constraint_map else AttributeConstraint.NULLABLE,
            constraints))


def from_string_to_attribute_type(attribute_type: str):
    attribute_types_map = {
        "STRING": AttributeType.STRING,
        "OBJECT": AttributeType.OBJECT,
        "NUMBER": AttributeType.NUMBER,
        "DATE": AttributeType.DATE,
        "ARRAY": AttributeType.ARRAY
    }
    return attribute_types_map[attribute_type] if attribute_type in attribute_types_map else AttributeType.STRING


def from_dict_to_client_authorization_http_signature(d: dict):
    client_scopes = list(map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
    return ClientAuthorizationHttpSignature(d["client_id"], client_scopes, d["public_key"])


def from_dict_to_client_authorization_api_key(d: dict):
    client_scopes = list(map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
    return ClientAuthorizationApiKey(d["client_id"], client_scopes, d["api_key"],
                                     d["whitelist_hosts"] if "whitelist_hosts" in d else None)


from_dict_to_client_authorization_factory = {
    "api_key": from_dict_to_client_authorization_api_key,
    "http_signature": from_dict_to_client_authorization_http_signature
}


def from_client_authorization_to_dict(client_authorization: ClientAuthorization):
    if isinstance(client_authorization, ClientAuthorizationApiKey):
        return from_client_authorization_api_key_to_dict(client_authorization)
    elif isinstance(client_authorization, ClientAuthorizationHttpSignature):
        return from_client_authorization_http_signature_to_dict(client_authorization)
    else:
        raise NotImplementedError("client_authorization not implemented")


def from_dict_to_client_authorization(d: dict):
    return from_dict_to_client_authorization_factory[d["type"]](d)


class SystemService:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_client_authorization(client_id: str):
        repository = DynamoPlusRepository(client_authorization_metadata, True)
        model = repository.get(client_id)
        if model:
            return from_dict_to_client_authorization(model.document)

    @staticmethod
    def create_collection(metadata: Collection):
        collection = from_collection_to_dict(metadata)
        repository = DynamoPlusRepository(collectionMetadata, True)
        model = repository.create(collection)
        return from_dict_to_collection(model.document)

    # def updateCollection(self, metadata:Collection):
    #     collection=self.fromCollectionToDict(metadata)
    #     repository = DynamoPlusRepository(collectionMetadata)
    #     model = repository.update(collection)
    #     return self.fromDictToCollection(model.document)
    @staticmethod
    def delete_collection(name: str):
        DynamoPlusRepository(collectionMetadata, True).delete(name)

    @staticmethod
    def get_all_collections(limit: int = None, start_from: str = None):
        index_metadata = Index(None, "collection", [])
        index_repository = IndexDynamoPlusRepository(collectionMetadata, True, index_metadata)
        last_evaluated_item = None
        if start_from:
            last_evaluated_item = index_repository.get(start_from)
        query: QueryRepository = QueryRepository(AnyMatch(), collectionMetadata, limit, last_evaluated_item)
        result = index_repository.query_v2(query)
        if result:
            return list(map(lambda m: from_dict_to_collection(m.document), result.data)), result.lastEvaluatedKey

    @staticmethod
    def get_collection_by_name(name: str):
        model = DynamoPlusRepository(collectionMetadata, True).get(name)
        if model:
            return from_dict_to_collection(model.document)

    @staticmethod
    def create_index(i: Index) -> Index:
        index = from_index_to_dict(i)
        repository = DynamoPlusRepository(index_metadata, True)
        model = repository.create(index)
        if model:
            created_index = from_dict_to_index(model.document)
            logger.info("index created {}".format(created_index.__str__()))
            index_by_collection_name = IndexDynamoPlusRepository(index_metadata,
                                                                 Index(None, "index", ["collection.name"]),
                                                                 True).create(model.document)
            logger.info(
                "{} has been indexed {}".format(created_index.collection_name, index_by_collection_name.document))
            index_by_name = IndexDynamoPlusRepository(index_metadata, Index(None, "index", ["collection.name", "name"]),
                                                      True).create(model.document)
            logger.info("{} has been indexed {}".format(created_index.collection_name, index_by_name.document))
            return created_index

    @staticmethod
    def get_index_matching_fiedls(fields: List[str], collection_name: str):
        index_name = "#".join(fields)
        return SystemService.get_index(index_name,collection_name)

    @staticmethod
    def get_index(name: str, collection_name: str):
        query: QueryRepository = QueryRepository(And([Eq("collection.name", collection_name), Eq("name", name)]),
                                                 index_metadata, 1)
        repository = DynamoPlusRepository(index_metadata, True)
        result = repository.query_v2(query)
        indexes = list(map(lambda m: from_dict_to_index(m.document), result.data))
        if len(indexes) == 0:
            return None
        else:
            return indexes[0]

    @staticmethod
    def delete_index(name: str):
        DynamoPlusRepository(index_metadata, True).delete(name)

    @staticmethod
    def get_indexes_from_collection_name_generator(collection_name: str, limit=10):
        has_more = True
        while has_more:
            last_evaluated_key = None
            indexes, last_evaluated_key = SystemService.find_indexes_from_collection_name(collection_name, limit,
                                                                                          last_evaluated_key)
            has_more = last_evaluated_key is not None
            for i in indexes:
                yield i

    @staticmethod
    def get_all_collections_generator(limit=10):
        has_more = True
        while has_more:
            last_evaluated_key = None
            collections, last_evaluated_key = SystemService.get_all_collections(limit, last_evaluated_key)
            has_more = last_evaluated_key is not None
            for c in collections:
                yield c

    @staticmethod
    def find_indexes_from_collection_name(collection_name: str, limit: int = None, start_from: str = None):
        repository = DynamoPlusRepository(index_metadata, True)
        last_evaluated_key = None
        if start_from:
            last_evaluated_key = repository.get(start_from)
        query: QueryRepository = QueryRepository(Eq("collection.name", collection_name),
                                                 index_metadata, limit, last_evaluated_key)
        result = repository.query_v2(query)
        return list(map(lambda m: from_dict_to_index(m.document), result.data)), result.lastEvaluatedKey

    @staticmethod
    def find_collections_by_example(example: Collection):
        repository = DynamoPlusRepository(collectionMetadata, True)
        query: QueryRepository = QueryRepository(Eq("name", example.name),
                                                 index_metadata)
        result = repository.query_v2(query)
        return list(map(lambda m: from_dict_to_collection(m.document), result.data))

    @staticmethod
    def create_client_authorization(client_authorization: ClientAuthorization):
        client_authorization_document = from_client_authorization_to_dict(client_authorization)
        logging.info("creating client authorization {}".format(str(client_authorization)))
        model = DynamoPlusRepository(client_authorization_metadata, True).create(client_authorization_document)
        if model:
            return from_dict_to_client_authorization(model.document)

    @staticmethod
    def update_authorization(client_authorization: ClientAuthorization):
        client_authorization_document = from_client_authorization_to_dict(client_authorization)
        logging.info("updating client authorization {}".format(str(client_authorization)))
        model = DynamoPlusRepository(client_authorization_metadata, True).update(client_authorization_document)
        if model:
            return from_dict_to_client_authorization(model.document)

    @staticmethod
    def delete_authorization(client_id: str):
        logging.info("deleting client authorization {}".format(client_id))
        DynamoPlusRepository(client_authorization_metadata, True).delete(client_id)
