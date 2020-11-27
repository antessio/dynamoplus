import logging
from typing import *

from dynamoplus.models.query.conditions import Predicate, AnyMatch, Eq, And
from dynamoplus.models.system.collection.collection import Collection, AttributeDefinition, AttributeType, \
    AttributeConstraint
from dynamoplus.models.system.index.index import Index
from dynamoplus.models.system.client_authorization.client_authorization import ClientAuthorization, \
    ClientAuthorizationApiKey, ClientAuthorizationHttpSignature, Scope, ScopesType
from dynamoplus.repository.models import QueryModel
from dynamoplus.utils.utils import get_values_by_key_recursive, convert_to_string
from dynamoplus.v2.repository.repositories import QueryRepository, get_table_name, Repository, Model, QueryResult
from dynamoplus.v2.service.model_service import  get_model,get_index_model,get_sk,get_pk

collection_metadata = Collection("collection", "name")
index_metadata = Collection("index", "uid")
client_authorization_metadata = Collection("client_authorization", "client_id")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Converter:

    @staticmethod
    def from_collection_to_dict(collection: Collection):
        result = {"name": collection.name, "id_key": collection.id_key}
        if collection.ordering_key:
            result["ordering_key"] = collection.ordering_key
        return result

    @staticmethod
    def from_index_to_dict(index: Index):
        return {"name": index.index_name, "collection": {"name": index.collection_name},
                "conditions": index.conditions}

    @staticmethod
    def from_dict_to_index(d: dict):
        return Index(d["uid"], d["collection"]["name"], d["conditions"],
                     d["ordering_key"] if "ordering_key" in d else None)

    @staticmethod
    def from_client_authorization_http_signature_to_dict(client_authorization: ClientAuthorizationHttpSignature):
        return {
            "type": "http_signature",
            "client_id": client_authorization.client_id,
            "client_scopes": list(map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                                      client_authorization.client_scopes)),
            "public_key": client_authorization.client_public_key
        }

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def from_collection_to_dict(collection: Collection):
        d = {
            "name": collection.name,
            "id_key": collection.id_key,
            "ordering": collection.ordering_key,
            "auto_generate_id": collection.auto_generate_id
        }
        if collection.attribute_definition:
            attributes = list(
                map(lambda a: Converter.from_attribute_definition_to_dict(a), collection.attribute_definition))
            d["attributes"] = attributes
        return d

    @staticmethod
    def from_dict_to_collection(d: dict):
        attributes = list(
            map(Converter.from_dict_to_attribute_definition, d["attributes"])) if "attributes" in d else None
        auto_generate_id = d["auto_generate_id"] if "auto_generate_id" in d else False
        return Collection(d["name"], d["id_key"], d["ordering"] if "ordering" in d else None, attributes,
                          auto_generate_id)

    @staticmethod
    def from_attribute_definition_to_dict(attribute: AttributeDefinition):
        nested_attributes = list(
            map(lambda a: Converter.from_attribute_definition_to_dict(a),
                attribute.attributes)) if attribute.attributes else None
        return {"name": attribute.name, "type": attribute.type.value, "attributes": nested_attributes}

    @staticmethod
    def from_dict_to_attribute_definition(d: dict):
        attributes = None
        if "attributes" in d and d["attributes"] is not None:
            attributes = list(map(Converter.from_dict_to_attribute_definition, d["attributes"]))
        return AttributeDefinition(d["name"], Converter.from_string_to_attribute_type(d["type"]),
                                   Converter.from_array_to_constraints_list(
                                       d["constraints"]) if "constraints" in d else None,
                                   attributes)

    @staticmethod
    def from_array_to_constraints_list(constraints: List[dict]):
        attribute_constraint_map = {
            "NULLABLE": AttributeConstraint.NULLABLE,
            "NOT_NULL": AttributeConstraint.NOT_NULL
        }
        return list(
            map(lambda c: attribute_constraint_map[
                c] if c in attribute_constraint_map else AttributeConstraint.NULLABLE,
                constraints))

    @staticmethod
    def from_string_to_attribute_type(attribute_type: str):
        attribute_types_map = {
            "STRING": AttributeType.STRING,
            "OBJECT": AttributeType.OBJECT,
            "NUMBER": AttributeType.NUMBER,
            "DATE": AttributeType.DATE,
            "ARRAY": AttributeType.ARRAY
        }
        return attribute_types_map[attribute_type] if attribute_type in attribute_types_map else AttributeType.STRING

    @staticmethod
    def from_dict_to_client_authorization_http_signature(d: dict):
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationHttpSignature(d["client_id"], client_scopes, d["public_key"])

    @staticmethod
    def from_dict_to_client_authorization_api_key(d: dict):
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationApiKey(d["client_id"], client_scopes, d["api_key"],
                                         d["whitelist_hosts"] if "whitelist_hosts" in d else None)

    @staticmethod
    def from_dict_to_client_authorization_factory():
        return {
            "api_key": Converter.from_dict_to_client_authorization_api_key,
            "http_signature": Converter.from_dict_to_client_authorization_http_signature
        }

    @staticmethod
    def from_client_authorization_to_dict(client_authorization: ClientAuthorization):
        if isinstance(client_authorization, ClientAuthorizationApiKey):
            return Converter.from_client_authorization_api_key_to_dict(client_authorization)
        elif isinstance(client_authorization, ClientAuthorizationHttpSignature):
            return Converter.from_client_authorization_http_signature_to_dict(client_authorization)
        else:
            raise NotImplementedError("client_authorization not implemented")

    @staticmethod
    def from_dict_to_client_authorization(d: dict):
        return Converter.from_dict_to_client_authorization_factory()[d["type"]](d)


class CollectionService:

    @staticmethod
    def get_collection(collection_name: str) -> Collection:
        repo = get_repository_factory(collection_metadata)
        model = get_model(collection_metadata, {"name": collection_name})
        result = repo.get(model.pk, model.sk)
        return Converter.from_dict_to_collection(result.document)

    @staticmethod
    def create_collection(collection: Collection) -> Collection:
        repo = get_repository_factory(collection_metadata)
        model = get_model(collection_metadata, Converter.from_collection_to_dict(collection))
        result = repo.create(model)
        return Converter.from_dict_to_collection(result.document)

    @staticmethod
    def delete_collection(collection_name: str):
        repo = get_repository_factory(collection_metadata)
        model = get_model(collection_metadata, {collection_metadata.id_key: collection_name})
        repo.delete(model.pk, model.sk)

    @staticmethod
    def get_all_collections(start_from: str = None, limit: int = None) -> (Collection, dict):
        result = QueryService.query_all(collection_metadata, limit, start_from)
        if result:
            return list(
                map(lambda m: Converter.from_dict_to_collection(m.document), result.data)), result.lastEvaluatedKey

    @staticmethod
    def get_all_collections_generator():
        has_more = True
        while has_more:
            last_evaluated_key = None
            collections, last_evaluated_key = CollectionService.get_all_collections(last_evaluated_key)
            has_more = last_evaluated_key is not None
            for c in collections:
                yield c


class IndexService:

    @staticmethod
    def create_index(index: Index) -> Index:
        index_dict = Converter.from_index_to_dict(index)

        query_result = QueryService.query_begins_with(index_metadata, Eq("name", index.index_name), ["name"])

        if len(query_result.data) != 0:
            return Converter.from_dict_to_index(query_result.data[0].document)

        repo = get_repository_factory(index_metadata)
        create_index_model = repo.create(get_model(index_metadata, index_dict))
        logger.info("index created {}".format(create_index_model.__str__()))
        if create_index_model:
            created_index = Converter.from_dict_to_index(create_index_model.document)
            index_by_collection_name_model = repo.create(
                get_index_model(index_metadata, Index(index.index_name, "index", ["collection.name"]), index_dict))
            logger.info(
                "{} has been indexed {}".format(created_index.collection_name, index_by_collection_name_model.document))
            index_by_name_model = repo.create(
                get_index_model(index_metadata,Index(index.index_name, "index", ["collection.name", "name"]), index_dict))
            logger.info("{} has been indexed {}".format(created_index.collection_name, index_by_name_model.document))
            return created_index

    @staticmethod
    def get_index_by_name_and_collection_name(name: str, collection_name: str):
        query_result = QueryService.query_begins_with(index_metadata,
                                                      And([Eq("collection.name", collection_name), Eq("name", name)]),
                                                      ["collection.name", "name"], None, 1)

        indexes = list(map(lambda m: Converter.from_dict_to_index(m.document), query_result.data))
        if len(indexes) == 0:
            return None
        else:
            return indexes[0]

    @staticmethod
    def get_index_by_collection_name(collection_name: str, start_from: str = None, limit: int = 20):
        query_result = QueryService.query_begins_with(index_metadata,
                                                      Eq("collection.name", collection_name),
                                                      ["collection.name"], start_from, limit)

        return list(map(lambda m: Converter.from_dict_to_index(m.document), query_result.data))

    @staticmethod
    def get_index_matching_fields(fields: List[str], collection_name: str, ordering_key: str = None):
        index_name = Index.index_name_generator(fields, ordering_key)
        index = IndexService.get_index_by_name_and_collection_name(index_name, collection_name)
        fields_counter = len(fields) - 1
        while index is None and fields_counter >= 1:
            index_name = Index.index_name_generator(fields[0:fields_counter], ordering_key)
            index = IndexService.get_index_by_name_and_collection_name(index_name, collection_name)
            fields_counter = fields_counter - 1
        return index

    @staticmethod
    def delete_index(name: str):
        repo = get_repository_factory(index_metadata)
        model = get_model(index_metadata, {index_metadata.id_key: name})
        repo.delete(model.pk, model.sk)

    @staticmethod
    def get_indexes_from_collection_name_generator(collection_name: str, limit=10):
        has_more = True
        while has_more:
            last_evaluated_key = None
            indexes, last_evaluated_key = IndexService.find_indexes_from_collection_name(collection_name, limit,
                                                                                         last_evaluated_key)
            has_more = last_evaluated_key is not None
            for i in indexes:
                yield i


class AuthorizationService:
    @staticmethod
    def get_client_authorization(id:str):
        repo = get_repository_factory(client_authorization_metadata)
        model = get_model(client_authorization_metadata, {client_authorization_metadata.id_key: id})
        result = repo.get(model.pk,model.sk)
        if result:
            return Converter.from_dict_to_client_authorization(result.document)

    @staticmethod
    def create_client_authorization(client_authorization: ClientAuthorization):
        client_authorization_document = Converter.from_client_authorization_to_dict(client_authorization)
        logging.info("creating client authorization {}".format(str(client_authorization)))
        repo = get_repository_factory(client_authorization_metadata)
        model = repo.create(get_model(client_authorization_metadata, client_authorization_document))
        if model:
            return Converter.from_dict_to_client_authorization(model.document)

    @staticmethod
    def update_authorization(client_authorization: ClientAuthorization):
        client_authorization_document = Converter.from_client_authorization_to_dict(client_authorization)
        logging.info("updating client authorization {}".format(str(client_authorization)))
        repo = get_repository_factory(client_authorization_metadata)
        model = repo.update(get_model(client_authorization_metadata, client_authorization_document))
        if model:
            return Converter.from_dict_to_client_authorization(model.document)

    @staticmethod
    def delete_authorization(client_id: str):
        logging.info("deleting client authorization {}".format(client_id))
        repo = get_repository_factory(client_authorization_metadata)
        model = get_model(client_authorization_metadata, {client_authorization_metadata.id_key: client_id})
        repo.delete(model.pk, model.sk)

def get_repository_factory(collection: Collection) -> Repository:
    return Repository(get_table_name(is_system(collection)))


def is_system(collection: Collection) -> bool:
    return collection.name in ["collection", "index", "client_authorization"]


class QueryService:
    @staticmethod
    def query_begins_with(collection: Collection, predicate: Predicate, fields: List[str], start_from: str = None , limit: int = 20) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        query_model = QueryModel(collection, fields, predicate)
        repo = QueryRepository(table_name)
        last_evaluated_item = None
        if start_from:
            last_evaluated_item = Repository(table_name).get(get_pk(collection, start_from), get_sk(collection))
        return repo.query_begins_with(query_model.sk(), query_model.data(), last_evaluated_item, limit)

    @staticmethod
    def query_all(collection: Collection, limit: int, start_from: str = None) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        repo = QueryRepository(table_name)
        last_evaluated_item = None
        if start_from:
            last_evaluated_item = Repository(table_name).get(get_pk(collection, start_from), get_sk(collection))
        query_model = QueryModel(collection, [], AnyMatch())
        return repo.query_all(query_model.sk(), last_evaluated_item, limit)



