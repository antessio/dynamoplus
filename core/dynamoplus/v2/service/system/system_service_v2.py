from __future__ import annotations

import abc
import logging
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import *

import dynamoplus.v2.repository.system_repositories
from dynamoplus.models.query.conditions import Eq, Predicate, Range, And
from dynamoplus.models.system.aggregation.aggregation import AggregationTrigger, \
    AggregationJoin, \
    AggregationType, Aggregation, AggregationCount, AggregationSum, AggregationAvg, AggregationConfiguration
from dynamoplus.models.system.client_authorization.client_authorization import ClientAuthorization, \
    ClientAuthorizationApiKey, ClientAuthorizationHttpSignature, Scope, ScopesType
from dynamoplus.models.system.collection.collection import AttributeDefinition, AttributeType, AttributeConstraint
from dynamoplus.models.system.index.index import IndexConfiguration
from dynamoplus.v2.repository.repositories_v2 import Counter, IndexingOperation, Model, RepositoryInterface, \
    EqCondition, AndCondition, BeginsWithCondition, AnyCondition
from dynamoplus.v2.repository.system_repositories import CollectionEntity, IndexEntity, \
    IndexByCollectionNameAndFieldsEntity, IndexByCollectionNameEntity, \
    ClientAuthorizationEntity, AggregationEntity, \
    AggregationConfigurationEntity, \
    AggregationConfigurationByCollectionNameEntity, \
    AggregationConfigurationByNameEntity, \
    AggregationByAggregationConfigurationNameEntity, ClientAuthorizationByClientId, AggregationIncrementCounter

CLIENT_AUTHORIZATION_ENTITY_NAME = "client_authorization"
CLIENT_AUTHORIZATION_ORDERING_KEY = "ordering"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

INDEX_COLLECTION = CollectionEntity("index", {
    "name": "index"
})


def generate_unique_ordering_key():
    timestamp = int(time.time())  # Get current timestamp
    unique_id = uuid.uuid4().hex  # Generate a unique identifier
    ordering_key = f"{timestamp}_{unique_id}"  # Combine timestamp and unique identifier
    return ordering_key


@dataclass(frozen=True)
class Index:
    id: uuid.UUID
    name: str
    collection_name: str
    conditions: List[str]
    index_configuration: IndexConfiguration = field(default=IndexConfiguration.OPTIMIZE_READ)
    ordering_key: str = None

    # def __post_init__(self):
    #     object.__setattr__(self, 'name', "{}__{}{}".format(self.collection_name,
    #                                                        "__".join(self.conditions),
    #                                                        "__ORDER_BY__" + self.ordering_key
    #                                                        if self.ordering_key is not None else ""))

    def to_dict(self) -> dict:
        d = {
            "id": str(self.id),
            "name": self.name,
            "collection": {"name": self.collection_name},
            "conditions": self.conditions
        }
        if self.ordering_key:
            d["ordering_key"] = self.ordering_key
        if self.index_configuration:
            d["configuration"] = self.index_configuration.name
        return d

    @staticmethod
    def from_dict(d: dict) -> Index:
        return Index(uuid.UUID(d["id"]), d["name"], d["collection"]["name"], d["conditions"],
                     IndexConfiguration(d["configuration"]) if "configuration" in d else None,
                     d["ordering_key"] if "ordering_key" in d else None)


class IndexEntityAdapter(IndexEntity):

    def __init__(self, index: Index, ordering: str):
        object_body = index.to_dict()
        if ordering:
            object_body["ordering"] = ordering
        super(IndexEntityAdapter, self).__init__(index.id, object_body)


@dataclass(frozen=True)
class IndexCreateCommand(abc.ABC):
    collection_name: str
    conditions: List[str]
    index_configuration: IndexConfiguration = field(default=IndexConfiguration.OPTIMIZE_READ)
    ordering_key: str = None

    def to_index_entity(self) -> IndexEntity:
        index_name = "{}__{}{}".format(self.collection_name, "__".join(self.conditions),
                                       "__ORDER_BY__" + self.ordering_key if self.ordering_key is not None else "")
        return IndexEntityAdapter(
            Index(uuid.uuid4(),
                  index_name,
                  self.collection_name, self.conditions,
                  self.index_configuration,
                  self.ordering_key), generate_unique_ordering_key())


@dataclass(frozen=True)
class Collection:
    name: str
    id_key: str
    ordering_key: str
    attributes: List[AttributeDefinition]
    auto_generated_id: bool

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "id_key": self.id_key,
            "ordering_key": self.ordering_key,
            "auto_generate_id": self.auto_generated_id
        }
        if self.attributes:
            attributes = list(
                map(lambda a: Collection.from_attribute_definition_to_dict(a), self.attributes))
            d["attributes"] = attributes
        return d

    @staticmethod
    def from_attribute_definition_to_dict(attribute: AttributeDefinition) -> dict:

        d = {
            "name": attribute.name,
            "type": attribute.type.value
        }
        if attribute.attributes:
            d["attributes"] = list(
                map(lambda a: Collection.from_attribute_definition_to_dict(a),
                    attribute.attributes))

        return d

    @staticmethod
    def from_dict(d: dict) -> Collection:
        attributes = list(
            map(Collection.from_dict_to_attribute_definition, d["attributes"])) if "attributes" in d else None
        auto_generate_id = d["auto_generate_id"] if "auto_generate_id" in d else False
        return Collection(d["name"], d["id_key"], d["ordering_key"] if "ordering_key" in d else None, attributes,
                          auto_generate_id)

    @staticmethod
    def from_dict_to_attribute_definition(d: dict) -> AttributeDefinition:
        attributes = None
        if "attributes" in d and d["attributes"] is not None:
            attributes = list(map(Collection.from_dict_to_attribute_definition, d["attributes"]))
        return AttributeDefinition(d["name"], Collection.from_string_to_attribute_type(d["type"]),
                                   Collection.from_array_to_constraints_list(
                                       d["constraints"]) if "constraints" in d else None,
                                   attributes)

    @staticmethod
    def from_string_to_attribute_type(attribute_type: str) -> AttributeType:
        attribute_types_map = {
            "STRING": AttributeType.STRING,
            "OBJECT": AttributeType.OBJECT,
            "NUMBER": AttributeType.NUMBER,
            "DATE": AttributeType.DATE,
            "ARRAY": AttributeType.ARRAY
        }
        return attribute_types_map[
            attribute_type] if attribute_type in attribute_types_map else AttributeType.STRING

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


@dataclass(frozen=True)
class ClientAuthorizationCreateCommand(abc.ABC):
    client_id: str
    client_scopes: List[Scope]

    @staticmethod
    def from_dict(d: dict) -> ClientAuthorizationCreateCommand:
        if d["type"] == 'api_key':
            return ClientAuthorizationApiKeyCreateCommand.from_dict(d)
        elif d["type"] == 'http_signature':
            return ClientAuthorizationHttpSignatureCreateCommand.from_dict(d)
        else:
            raise ValueError('{} is not supported'.format(d["type"]))

    def to_client_authorization_entity(self) -> ClientAuthorizationEntity:
        uid = uuid.uuid4()
        ordering = generate_unique_ordering_key()
        if isinstance(self, ClientAuthorizationApiKeyCreateCommand):
            return ClientAuthorizationEntityAdapter(
                ClientAuthorizationApiKey(uid, self.client_id,
                                          self.client_scopes,
                                          self.api_key,
                                          self.whitelist_hosts), ordering)
        elif isinstance(self, ClientAuthorizationHttpSignatureCreateCommand):
            return ClientAuthorizationEntityAdapter(ClientAuthorizationHttpSignature(uid,
                                                                                     self.client_id,
                                                                                     self.client_scopes,
                                                                                     self.client_public_key),
                                                    ordering)
        raise ValueError("{} not supported".format(self.__class__))


@dataclass(frozen=True)
class ClientAuthorizationApiKeyCreateCommand(ClientAuthorizationCreateCommand):
    api_key: str
    whitelist_hosts: List[str]

    def to_dict(self) -> dict:
        result = {
            "type": "api_key",
            "client_id": self.client_id,
            "client_scopes": list(
                map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                    self.client_scopes)),
            "api_key": self.api_key,
        }
        if self.whitelist_hosts:
            result["whitelist_hosts"] = self.whitelist_hosts
        return result

    @staticmethod
    def from_dict(d: dict) -> ClientAuthorizationApiKeyCreateCommand:
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationApiKeyCreateCommand(d["client_id"], client_scopes, d["api_key"],
                                                      d["whitelist_hosts"] if "whitelist_hosts" in d else None)


@dataclass(frozen=True)
class ClientAuthorizationHttpSignatureCreateCommand(ClientAuthorizationCreateCommand):
    client_public_key: str

    def to_dict(self) -> dict:
        return {
            "type": "http_signature",
            "client_id": self.client_id,
            "client_scopes": list(
                map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                    self.client_scopes)),
            "public_key": self.client_public_key
        }

    @staticmethod
    def from_dict(d: dict) -> ClientAuthorizationHttpSignatureCreateCommand:
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationHttpSignatureCreateCommand(d["client_id"], client_scopes, d["public_key"])


@dataclass(frozen=True)
class ClientAuthorization(abc.ABC):
    id: uuid.UUID
    client_id: str
    client_scopes: List[Scope]

    def __post_init__(self):
        if self.id is None:
            object.__setattr__(self, 'id', uuid.uuid4())

    @abc.abstractmethod
    def to_dict(self) -> dict:
        pass

    @staticmethod
    def from_dict(d: dict) -> ClientAuthorization:
        if d["type"] == 'api_key':
            return ClientAuthorizationApiKey.from_dict(d)
        elif d["type"] == 'http_signature':
            return ClientAuthorizationHttpSignature.from_dict(d)
        else:
            raise ValueError('{} is not supported'.format(d["type"]))


@dataclass(frozen=True)
class ClientAuthorizationApiKey(ClientAuthorization):
    api_key: str
    whitelist_hosts: List[str]

    def to_dict(self) -> dict:
        result = {
            "id": str(self.id),
            "type": "api_key",
            "client_id": self.client_id,
            "client_scopes": list(
                map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                    self.client_scopes)),
            "api_key": self.api_key,
        }
        if self.whitelist_hosts:
            result["whitelist_hosts"] = self.whitelist_hosts
        return result

    @staticmethod
    def from_dict(d: dict) -> ClientAuthorizationApiKey:
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationApiKey(uuid.UUID(d["id"]), d["client_id"], client_scopes, d["api_key"],
                                         d["whitelist_hosts"] if "whitelist_hosts" in d else None)


@dataclass(frozen=True)
class ClientAuthorizationHttpSignature(ClientAuthorization):
    client_public_key: str

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "type": "http_signature",
            "client_id": self.client_id,
            "client_scopes": list(
                map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                    self.client_scopes)),
            "public_key": self.client_public_key
        }

    @staticmethod
    def from_dict(d: dict) -> ClientAuthorizationHttpSignature:
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationHttpSignature(uuid.UUID(d["id"]), d["client_id"], client_scopes, d["public_key"])


class ClientAuthorizationEntityAdapter(ClientAuthorizationEntity):
    def __init__(self, client_authorization: ClientAuthorization, ordering=None):
        object_body = client_authorization.to_dict()
        if ordering:
            object_body[CLIENT_AUTHORIZATION_ORDERING_KEY] = ordering
        super(ClientAuthorizationEntityAdapter, self).__init__(client_authorization.id,
                                                               object_body)


@dataclass(frozen=True)
class Aggregation(abc.ABC):
    id: uuid.UUID
    name: str
    configuration_name: str

    @abc.abstractmethod
    def to_dict(self) -> dict:
        pass

    @staticmethod
    def from_dict(d: dict) -> Aggregation:
        uid = uuid.UUID(d["id"])
        name = d["name"]
        configuration = d["configuration_name"]
        if "count" in d:
            return AggregationCount(uid, name, configuration, d["count"])
        elif "sum" in d:
            return AggregationSum(uid, name, configuration, d["sum"])
        elif "avg" in d:
            return AggregationAvg(uid, name, configuration, d["avg"])
        else:
            raise ValueError('{} is not supported'.format(configuration))


@dataclass(frozen=True)
class AggregationCount(Aggregation):
    count: int

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "name": self.name,
            "configuration_name": self.configuration_name,
            "count": self.count,
            "type": "COLLECTION_COUNT"}


@dataclass(frozen=True)
class AggregationSum(Aggregation):
    sum: float

    def to_dict(self) -> dict:
        return {"id": str(self.id), "name": self.name, "configuration_name": self.configuration_name,
                "sum": self.sum,
                "type": "SUM"}


@dataclass(frozen=True)
class AggregationAvg(Aggregation):
    avg: float

    def to_dict(self) -> dict:
        return {"id": str(self.id), "name": self.name, "configuration_name": self.configuration_name,
                "avg": Decimal(self.avg),
                "type": "AVG"}


@dataclass(frozen=True)
class AggregationCreateCommand(abc.ABC):
    name: str
    configuration_name: str

    @abc.abstractmethod
    def to_dict(self) -> dict:
        pass

    @staticmethod
    def from_dict(d: dict) -> AggregationCreateCommand:
        name = d["name"]
        configuration = d["configuration_name"]
        if "count" in d:
            return AggregationCountCreateCommand(name, configuration, d["count"])
        elif "sum" in d:
            return AggregationSumCreateCommand(name, configuration, d["sum"])
        elif "avg" in d:
            return AggregationAvgCreateCommand(name, configuration, d["avg"])
        else:
            raise ValueError('{} is not supported'.format(configuration))


@dataclass(frozen=True)
class AggregationCountCreateCommand(AggregationCreateCommand):
    count: int

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "configuration_name": self.configuration_name,
            "count": self.count,
            "type": "COLLECTION_COUNT"}


@dataclass(frozen=True)
class AggregationSumCreateCommand(AggregationCreateCommand):
    sum: float

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "configuration_name": self.configuration_name,
            "sum": self.sum,
            "type": "SUM"}


@dataclass(frozen=True)
class AggregationAvgCreateCommand(AggregationCreateCommand):
    avg: float

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "configuration_name": self.configuration_name,
            "avg": Decimal(self.avg),
            "type": "AVG"}


@dataclass(frozen=True)
class AggregationConfiguration:
    uid: uuid.UUID
    name: str = field(init=False, repr=True)
    collection_name: str
    type: AggregationType
    on: List[AggregationTrigger]
    target_field: str
    matches: Predicate
    join: AggregationJoin = None

    def __post_init__(self):
        matches_part = ""
        join_part = ""
        target_part = ""
        if self.target_field:
            target_part = "_{}".format(self.target_field)
        if self.matches:
            matches_part = "_{}".format("_".join(self.matches.get_fields() + self.matches.get_values()))
        if self.join:
            join_part = "by_{}".format(self.join.collection_name)
        name = "{}{}_{}{}{}".format(self.collection_name, matches_part, self.type.name.lower(), target_part,
                                    join_part)
        object.__setattr__(self, 'name', name)

    @staticmethod
    def from_dict_to_predicate(d: dict):
        if "eq" in d:
            return Eq(d["eq"]["field_name"], d["eq"]["value"])
        elif "range" in d:
            return Range(d["range"]["field_name"], d["range"]["from"], d["range"]["to"])
        elif "and" in d:
            conditions = list(map(lambda cd: AggregationConfiguration.from_dict_to_predicate(cd), d["and"]))
            return And(conditions)

    @staticmethod
    def from_dict(document: dict) -> AggregationConfiguration:
        collection_name = document["collection"]["name"]
        t = AggregationType.value_of(document["type"])
        inner_aggregation_document = document["aggregation"] if 'aggregation' in document else None
        on = list(map(lambda o: AggregationTrigger.value_of(o),
                      inner_aggregation_document["on"])) if inner_aggregation_document else None
        target_field = None
        matches = None
        join = None
        if inner_aggregation_document and "target_field" in inner_aggregation_document:
            target_field = inner_aggregation_document["target_field"]
        if inner_aggregation_document and "join" in inner_aggregation_document:
            join = AggregationJoin(inner_aggregation_document["join"]["collection_name"],
                                   inner_aggregation_document["join"]["using_field"])
        if inner_aggregation_document and "matches" in inner_aggregation_document:
            matches = AggregationConfiguration.from_dict_to_predicate(inner_aggregation_document["matches"])

        uid = uuid.UUID(document["id"])
        return AggregationConfiguration(uid, collection_name, t, on, target_field, matches, join)

    def to_dict(self) -> dict:
        a = {
            "on": list(map(lambda o: o.name, self.on)),
            "name": self.name
        }
        d = {
            "collection": {
                "name": self.collection_name
            },
            "type": self.type.name
        }
        if self.join:
            a["join"] = {
                "collection_name": self.join.collection_name,
                "using_field": self.join.using_field
            }
        if self.target_field:
            a["target_field"] = self.target_field
        if self.matches:
            a["matches"] = AggregationConfiguration.from_predicate_to_dict(self.matches)
        d["aggregation"] = a
        d["id"] = str(self.uid)
        return d

    @staticmethod
    def from_predicate_to_dict(predicate: Predicate):
        if isinstance(predicate, Eq):
            return {"eq": {"field_name": predicate.field_name, "value": predicate.value}}
        elif isinstance(predicate, Range):
            return {
                "range": {"field_name": predicate.field_name, "from": predicate.from_value,
                          "to": predicate.to_value}}
        elif isinstance(predicate, And):
            return {
                "and": list(
                    map(lambda c: AggregationConfiguration.from_predicate_to_dict(c), predicate.eq_conditions))}


@dataclass(frozen=True)
class AggregationConfigurationCreateCommand:
    collection_name: str
    type: AggregationType
    on: List[AggregationTrigger]
    target_field: str
    matches: Predicate
    join: AggregationJoin = None

    def to_dict(self):
        a = {
            "on": list(map(lambda o: o.name, self.on)),
        }
        d = {
            "collection": {
                "name": self.collection_name
            },
            "type": self.type.name
        }
        if self.join:
            a["join"] = {
                "collection_name": self.join.collection_name,
                "using_field": self.join.using_field
            }
        if self.target_field:
            a["target_field"] = self.target_field
        if self.matches:
            a["matches"] = AggregationConfiguration.from_predicate_to_dict(self.matches)
        d["aggregation"] = a
        return d


class AggregationConfigurationEntityAdapter(AggregationConfigurationEntity):
    def __init__(self, aggregation_configuration: AggregationConfiguration):
        super(AggregationConfigurationEntityAdapter, self).__init__(aggregation_configuration.uid,
                                                                    aggregation_configuration.to_dict())


class CollectionService:

    def __init__(self, repository: RepositoryInterface):
        self.repo = repository

    def get_collection(self, collection_name: str) -> Collection:
        result = self.repo.get(collection_name, 'collection')
        if result:
            return Collection.from_dict(result)

    def create_collection(self, collection: Collection) -> Collection:
        object_body = collection.to_dict()
        object_body['ordering'] = generate_unique_ordering_key()
        result = self.repo.create(CollectionEntity(collection.name, object_body))
        return Collection.from_dict(result)

    def delete_collection(self, collection_name: str) -> None:
        self.repo.delete(collection_name, 'collection')

    def get_all_collections(self, limit: int = None, start_from: str = None) -> ([Collection], str):
        starting_from_collection = None
        if start_from:
            starting_from_collection = self.repo.get(start_from, 'collection')
        result, last_evaluated_id = self.repo.query(
            dynamoplus.v2.repository.system_repositories.COLLECTION_ENTITY_NAME,
            AnyCondition(), limit, starting_from_collection)

        if result:
            return list(
                map(lambda m: Collection.from_dict(m), result)), last_evaluated_id
        else:
            return [], None

    def get_all_collections_generator(self) -> Generator[Collection]:
        has_more = True
        while has_more:
            last_evaluated_key = None
            collections, last_evaluated_key = self.get_all_collections(start_from=last_evaluated_key)
            has_more = last_evaluated_key is not None
            for c in collections:
                yield c


class IndexService:

    def __init__(self, repository: RepositoryInterface):
        self.repo = repository

    def create_index(self, create_index_command: IndexCreateCommand) -> Index:

        logger.debug("index : {}".format(create_index_command.__str__()))

        existing_index = self.get_index_by_collection_name_and_conditions(create_index_command.collection_name,
                                                                          create_index_command.conditions)

        if existing_index:
            return existing_index

        index_entity = create_index_command.to_index_entity()
        model = self.repo.create(index_entity)
        logger.info("index created {}".format(model.__str__()))
        if model:
            created_index = Index.from_dict(model)
            create_index_entity = IndexEntity.from_dict(model)
            self.repo.indexing(IndexingOperation([],
                                                 [],
                                                 [
                                                     IndexByCollectionNameAndFieldsEntity(
                                                         created_index.id,
                                                         created_index.collection_name,
                                                         created_index.conditions,
                                                         create_index_entity.object(),
                                                         create_index_entity.ordering()),
                                                     IndexByCollectionNameEntity(created_index.id,
                                                                                 created_index.collection_name,
                                                                                 create_index_entity.object(),
                                                                                 create_index_entity.ordering())]))
            return created_index

    def get_index_by_collection_name_and_conditions(self, collection_name: str, fields: List[str]) -> Index | None:
        query = AndCondition([EqCondition("collection.name", collection_name)],
                             BeginsWithCondition("fields", "__".join(fields)))
        result, last_key = self.repo.query(dynamoplus.v2.repository.system_repositories.INDEX_ENTITY_NAME, query, 1,
                                           None)
        if result and len(result) == 1:
            return Index.from_dict(result[0])
        else:
            return None

    def get_index_by_collection_name(self, collection_name: str, limit: int = 20, start_from: uuid.UUID = None) -> (
            List[Index], uuid.UUID):
        last_index_entity = None
        if start_from:
            last_index_entity = self.__get_index_entity_by_id(start_from)

        data, last_evaluated_key = self.repo.query(dynamoplus.v2.repository.system_repositories.INDEX_ENTITY_NAME,
                                                   BeginsWithCondition("collection.name", collection_name), limit,
                                                   last_index_entity)

        return list(
            map(lambda m: Index.from_dict(m), data)), uuid.UUID(
            last_evaluated_key) if last_evaluated_key else None

    def get_index_matching_fields(self, fields: List[str], collection_name: str):

        index = self.get_index_by_collection_name_and_conditions(collection_name, fields)
        fields_counter = len(fields) - 1
        while index is None and fields_counter >= 1:
            index = self.get_index_by_collection_name_and_conditions(collection_name, fields[0:fields_counter])
            fields_counter = fields_counter - 1
        return index

    def delete_index(self, uid: uuid.UUID) -> None:
        self.repo.delete(IndexEntity(uid))

    def get_indexes_from_collection_name_generator(self, collection_name: str):
        has_more = True
        batch_size = 20
        while has_more:
            last_evaluated_key = None
            indexes, last_evaluated_key = self.get_index_by_collection_name(collection_name, batch_size,
                                                                            last_evaluated_key)
            has_more = last_evaluated_key is not None
            for i in indexes:
                yield i

    def get_index_by_id(self, uid: uuid.UUID) -> Index:
        index_model = self.__get_index_entity_by_id(uid)
        return Index.from_dict(index_model)

    def __get_index_entity_by_id(self, uid: uuid.UUID) -> dict:
        return self.repo.get(str(uid), 'index')

    def get_all_indexes(self, limit: int, start_from: uuid.UUID):
        start_from_entity = None
        if start_from:
            start_from_entity = self.get_index_by_id(start_from)
        result, last_key = self.repo.query(dynamoplus.v2.repository.system_repositories.INDEX_ENTITY_NAME,
                                           AnyCondition(), limit, start_from_entity)
        if result:
            return list(
                map(lambda m: Index.from_dict(m), result)), last_key
        else:
            return [], None


class AuthorizationService:

    def __init__(self, repo: RepositoryInterface):
        self.repo = repo

    def get_client_authorization(self, uid: uuid.UUID):
        result = self.repo.get(str(uid), CLIENT_AUTHORIZATION_ENTITY_NAME)
        if result:
            return ClientAuthorization.from_dict(result)

    def create_client_authorization(self, client_authorization_create_command: ClientAuthorizationCreateCommand):
        logging.info("creating client authorization {}".format(str(client_authorization_create_command)))

        client_authorization_to_create = client_authorization_create_command.to_client_authorization_entity()

        model = self.repo.create(client_authorization_to_create)
        if model:
            created_client_authorization_entity = ClientAuthorizationEntity.from_dict(model)
            created_client_authorization = ClientAuthorization.from_dict(model)
            self.repo.indexing(IndexingOperation([],
                                                 [],
                                                 [
                                                     ClientAuthorizationByClientId(
                                                         created_client_authorization.id,
                                                         created_client_authorization.client_id,
                                                         created_client_authorization_entity.object(),
                                                         created_client_authorization_entity.ordering())]))
            return created_client_authorization

    def get_client_authorization_by_client_id(self, client_id: str, limit: int,
                                              last_client_authorization_id: uuid = None):

        data, last_evaluated_key = self.repo.query(CLIENT_AUTHORIZATION_ENTITY_NAME,
                                                   BeginsWithCondition("client_id", client_id + "#"), limit,
                                                   str(last_client_authorization_id) if last_client_authorization_id else None)

        return list(
            map(lambda m: ClientAuthorization.from_dict(m), data)), uuid.UUID(
            last_evaluated_key) if last_evaluated_key else None

    def update_authorization(self, client_authorization: ClientAuthorization):

        logging.info("updating client authorization {}".format(str(client_authorization)))

        model = self.repo.update(ClientAuthorizationEntityAdapter(client_authorization))
        if model:
            return ClientAuthorization.from_dict(model)

    def delete_authorization(self, client_id: uuid.UUID):
        logging.info("deleting client authorization {}".format(client_id))
        self.repo.delete(str(client_id), "client_authorization")


class AggregationService:

    def __init__(self, repo: RepositoryInterface):
        self.repo = repo

    def get_aggregation_by_id(self, uid: uuid.UUID) -> Aggregation:

        result = self.__get_aggregation_entity_by_id(uid)
        if result:
            return Aggregation.from_dict(result)

    def __get_aggregation_entity_by_id(self, uid: uuid.UUID):
        return self.repo.get(str(uid), 'aggregation')

    def get_aggregations_by_configuration_name(self, configuration_name: str, limit: int = 20,
                                               start_from: uuid.UUID = None) -> \
            Tuple[
                List[Union[Aggregation]], uuid.UUID]:
        start_from_entity = None
        if start_from:
            start_from_entity = self.__get_aggregation_entity_by_id(start_from)

        result, last_key = self.repo.query(dynamoplus.v2.repository.system_repositories.AGGREGATION_ENTITY_NAME,
                                           BeginsWithCondition("configuration_name", configuration_name), limit,
                                           start_from_entity)

        if result:
            return list(
                map(lambda m: self.get_aggregation_by_id(uuid.UUID(m['id'])), result)), last_key
        else:
            return [], None

    def get_aggregation_by_configuration_name(self, configuration_name: str) -> Aggregation:
        result, last_key = self.repo.query(dynamoplus.v2.repository.system_repositories.AGGREGATION_ENTITY_NAME,
                                           EqCondition("aggregation_configuration", configuration_name),
                                           1,
                                           None)
        if result and len(result) == 1:
            return self.get_aggregation_by_id(uuid.UUID(result[0].id()))

    def get_aggregations_by_configuration_name_generator(self, configuration_name: str) -> Generator[Aggregation]:
        has_more = True
        start_from = None
        while has_more:
            result, last_key = self.get_aggregations_by_configuration_name(configuration_name, 20,
                                                                           start_from)
            has_more = last_key is not None
            for c in result:
                yield c
            start_from = last_key

    def get_all_aggregations(self, limit: int, start_from: uuid.UUID) -> Tuple[List[Union[Aggregation]], uuid.UUID]:
        start_from_entity = None
        if start_from:
            start_from_entity = self.__get_aggregation_entity_by_id(start_from)
        result, last_key = self.repo.query(dynamoplus.v2.repository.system_repositories.AGGREGATION_ENTITY_NAME,
                                           AnyCondition(), limit, start_from_entity)
        if result:
            return list(
                map(lambda m: Aggregation.from_dict(m), result)), last_key
        else:
            return [], None

    def increment_count(self, aggregation: AggregationCount) -> Aggregation:
        self.repo.increment_count(AggregationIncrementCounter(str(aggregation.id), 'count', Decimal(1)))
        return AggregationCount(aggregation.id, aggregation.name, aggregation.configuration_name,
                                aggregation.count + 1)

    def increment(self, aggregation: Aggregation, new_value: Decimal) -> Aggregation:

        entity = AggregationEntity(aggregation.id, aggregation.to_dict())
        counter, converter = AggregationService.__get_increment(aggregation, new_value)
        if counter.count != 0:
            result = self.repo.increment_count(AggregationIncrementCounter(str(aggregation.id), 'count', counter.count))
            if result:
                return converter(aggregation)
            else:
                return aggregation
        else:
            return aggregation

    @staticmethod
    def __get_increment(aggregation: Aggregation, new_value: Decimal) -> (
            Counter, Callable[[Aggregation], Aggregation]):

        if isinstance(aggregation, AggregationCount):
            delta = new_value - aggregation.count
            return Counter('count', delta, True if delta >= 0 else False), lambda a: \
                AggregationCount(aggregation.id,
                                 aggregation.name,
                                 aggregation.configuration_name,
                                 int(abs(delta.to_integral())))
        elif isinstance(aggregation, AggregationSum):
            delta = new_value - aggregation.sum
            return Counter('sum', delta, True if delta >= 0 else False), lambda a: \
                AggregationSum(aggregation.id,
                               aggregation.name,
                               aggregation.configuration_name,
                               float(abs(delta)))
        elif isinstance(aggregation, AggregationAvg):
            delta = new_value - Decimal(aggregation.avg)
            return Counter('avg', delta, True if delta >= 0 else False), lambda a: \
                AggregationAvg(aggregation.id,
                               aggregation.name,
                               aggregation.configuration_name,
                               float(abs(delta)))
        else:
            raise ValueError("invalid aggregation {}".format(aggregation))

    def decrement_count(self, aggregation: AggregationCount) -> Aggregation:
        entity = AggregationEntity(aggregation.id, aggregation.to_dict()).to_dynamo_db_model()
        self.repo.increment_counter(entity.to_dynamo_db_item(), [Counter("count", Decimal(1), False)])
        return AggregationCount(aggregation.id, aggregation.name, aggregation.configuration_name,
                                aggregation.count - 1)

    def create_aggregation(self, aggregation_create_command: AggregationCreateCommand) -> Aggregation:
        aggregation_create_command_dict = aggregation_create_command.to_dict()
        uid = uuid.uuid4()
        aggregation_create_command_dict['ordering'] = generate_unique_ordering_key()
        aggregation_create_command_dict['id'] = str(uid)

        model = self.repo.create(AggregationEntity(uid, aggregation_create_command_dict))
        if model:
            created_aggregation_entity = AggregationEntity.from_dict(model)
            created_aggregation = Aggregation.from_dict(model)
            self.repo.indexing(IndexingOperation([],
                                                 [],
                                                 [AggregationByAggregationConfigurationNameEntity(
                                                     created_aggregation.id,
                                                     created_aggregation.configuration_name,
                                                     model,
                                                     created_aggregation_entity.ordering())]))

            return created_aggregation

    def update_aggregation(self, aggregation: Aggregation) -> Aggregation:
        updated_entity = self.repo.update(AggregationEntity(aggregation.id, aggregation.to_dict()))
        return Aggregation.from_dict(updated_entity)


class AggregationConfigurationService:

    def __from_entity_to_aggregation_configuration(m: dict):
        return AggregationConfiguration.from_dict(m)

    def __init__(self, repo: RepositoryInterface):
        self.repo = repo

    def __get_aggregation_configuration_entity_by_id(self, uid: uuid.UUID) -> dict:
        return self.repo.get(str(uid), 'aggregation_configuration')

    def get_aggregation_configuration_by_uid(self, uid: uuid.UUID):
        entity = self.__get_aggregation_configuration_entity_by_id(uid)
        return AggregationConfigurationService.__from_entity_to_aggregation_configuration(entity)

    def get_all_aggregation_configurations(self, limit: int, start_from: uuid.UUID) -> \
            (List[AggregationConfiguration], uuid.UUID):
        start_after_entity = None
        if start_from:
            start_after_entity = self.__get_aggregation_configuration_entity_by_id(start_from)
        result, last_evaluated_key = self.repo.query(
            dynamoplus.v2.repository.system_repositories.AGGREGATION_CONFIGURATION_ENTITY_NAME,
            AnyCondition(),
            limit,
            start_after_entity)
        if result:
            return list(map(lambda m: AggregationConfigurationService.__from_entity_to_aggregation_configuration(m),
                            result)), uuid.UUID(last_evaluated_key) if last_evaluated_key is not None else None
        else:
            return [], None

    def create_aggregation_configuration(self,
                                         aggregation_configuration_create_command: AggregationConfigurationCreateCommand):
        aggregation_configuration_to_dict = aggregation_configuration_create_command.to_dict()
        uid = uuid.uuid4()
        aggregation_configuration_to_dict['ordering'] = generate_unique_ordering_key()
        aggregation_configuration_to_dict['name'] = self.generate_aggregation_configuration_name(
            aggregation_configuration_create_command)
        aggregation_configuration_to_dict['id'] = str(uid)
        model = self.repo.create(AggregationConfigurationEntity(uid, aggregation_configuration_to_dict))
        created_aggregation = AggregationConfigurationService.__from_entity_to_aggregation_configuration(model)
        created_aggregation_entity = AggregationEntity.from_dict(model)
        self.repo.indexing(IndexingOperation([],
                                             [],
                                             [AggregationConfigurationByCollectionNameEntity(
                                                 created_aggregation.uid,
                                                 created_aggregation.collection_name,
                                                 model,
                                                 created_aggregation_entity.ordering()),
                                                 AggregationConfigurationByNameEntity(created_aggregation.uid,
                                                                                      created_aggregation.name,
                                                                                      model,
                                                                                      created_aggregation_entity.ordering())]))
        return created_aggregation

    def generate_aggregation_configuration_name(self,
                                                aggregation_configuration: AggregationConfigurationCreateCommand) -> str:
        join_part = ""
        target_part = ""
        matches_part = ""
        if aggregation_configuration.target_field:
            target_part = "_{}".format(aggregation_configuration.target_field)
        if aggregation_configuration.matches:
            matches_part = "_{}".format("_".join(
                aggregation_configuration.matches.get_fields() + aggregation_configuration.matches.get_values()))
        if aggregation_configuration.join:
            join_part = "by_{}".format(aggregation_configuration.join.collection_name)
        return "{}{}_{}{}{}".format(aggregation_configuration.collection_name, matches_part,
                                    aggregation_configuration.type.name.lower(), target_part,
                                    join_part)

    def get_aggregation_configurations_by_collection_name_generator(self, collection_name: str) \
            -> Generator[AggregationConfiguration]:
        has_more = True
        batch_size = 20
        while has_more:
            last_evaluated_key = None
            indexes, last_evaluated_key = self.get_aggregation_configurations_by_collection_name(collection_name,
                                                                                                 batch_size,
                                                                                                 last_evaluated_key)
            has_more = last_evaluated_key is not None
            for i in indexes:
                yield i

    def get_aggregation_configurations_by_collection_name(self,
                                                          collection_name: str,
                                                          limit: int = 20,
                                                          start_from: uuid.UUID = None) -> \
            (List[AggregationConfiguration], uuid.UUID):
        start_after_entity = None
        if start_from:
            start_after_entity = self.__get_aggregation_configuration_entity_by_id(start_from)
        result, last_evaluated_key = self.repo.query(
            dynamoplus.v2.repository.system_repositories.AGGREGATION_CONFIGURATION_ENTITY_NAME,
            BeginsWithCondition("collection.name", collection_name),
            limit, start_after_entity)
        if result:
            return list(map(lambda m: AggregationConfigurationService.__from_entity_to_aggregation_configuration(m),
                            result)), uuid.UUID(last_evaluated_key) if last_evaluated_key is not None else None
        else:
            return [], None

    def get_aggregation_configurations_by_name(self,
                                               name: str,
                                               limit: int = 20,
                                               start_from: uuid.UUID = None) -> \
            (List[AggregationConfiguration], uuid.UUID):
        start_after_entity = None
        if start_from:
            start_after_entity = self.__get_aggregation_configuration_entity_by_id(start_from)
        result, last_evaluated_key = self.repo.query(
            dynamoplus.v2.repository.system_repositories.AGGREGATION_CONFIGURATION_ENTITY_NAME,
            EqCondition("name", name),
            limit, start_after_entity)
        if result:
            return list(map(lambda m: AggregationConfigurationService.__from_entity_to_aggregation_configuration(m),
                            result)), uuid.UUID(last_evaluated_key) if last_evaluated_key is not None else None
        else:
            return [], None
