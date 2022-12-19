from __future__ import annotations

import abc
import logging
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import *

from aws.dynamodb.dynamodbdao import Counter
from dynamoplus.models.query.conditions import Eq, Predicate, Range, And
from dynamoplus.models.system.aggregation.aggregation import AggregationConfiguration, AggregationTrigger, \
    AggregationJoin, \
    AggregationType, Aggregation, AggregationCount, AggregationSum, AggregationAvg
from dynamoplus.models.system.client_authorization.client_authorization import ClientAuthorization, \
    ClientAuthorizationApiKey, ClientAuthorizationHttpSignature, Scope, ScopesType
from dynamoplus.models.system.collection.collection import AttributeDefinition, AttributeType, AttributeConstraint
from dynamoplus.models.system.index.index import IndexConfiguration
from dynamoplus.v2.repository import repositories_v2
from dynamoplus.v2.repository.repositories_v2 import DynamoDBRepository, QueryAll, IndexingOperation, Model
from dynamoplus.v2.repository.system_repositories import CollectionEntity, IndexEntity, \
    IndexByCollectionNameAndFieldsEntity, QueryIndexByCollectionNameAndFields, IndexByCollectionNameEntity, \
    QueryIndexByCollectionName, ClientAuthorizationEntity, AggregationEntity, \
    QueryAggregationByAggregationConfigurationName, AggregationConfigurationEntity, \
    QueryAggregationConfigurationByCollectionName
from dynamoplus.v2.service.common import SingletonMeta

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass(frozen=True)
class Index:
    id: uuid.UUID
    collection_name: str
    conditions: List[str]
    index_configuration: IndexConfiguration = field(default=IndexConfiguration.OPTIMIZE_READ)
    ordering_key: str = None
    name: str = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, 'name', "{}__{}{}".format(self.collection_name,
                                                           "__".join(self.conditions),
                                                           "__ORDER_BY__" + self.ordering_key
                                                           if self.ordering_key is not None else ""))

    def to_dict(self) -> dict:
        d = {
            "id": str(self.id),
            "name": self.name,
            "collection": {"name": self.collection_name},
            "conditions": self.conditions}
        if self.ordering_key:
            d["ordering_key"] = self.ordering_key
        if self.index_configuration:
            d["configuration"] = self.index_configuration.name
        return d

    @staticmethod
    def from_dict(d: dict) -> Index:
        return Index(uuid.UUID(d["id"]), d["collection"]["name"], d["conditions"],
                     IndexConfiguration(d["configuration"]) if "configuration" in d else None,
                     d["ordering_key"] if "ordering_key" in d else None)


class IndexEntityAdapter(IndexEntity):

    def __init__(self, index: Index):
        super(IndexEntityAdapter, self).__init__(index.id, index.to_dict())


@dataclass(frozen=True)
class Collection:
    name: str
    id_key: str
    ordering: str
    attributes: List[AttributeDefinition]
    auto_generated_id: bool

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "id_key": self.id_key,
            "ordering": self.ordering,
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
        return Collection(d["name"], d["id_key"], d["ordering"] if "ordering" in d else None, attributes,
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
        return attribute_types_map[attribute_type] if attribute_type in attribute_types_map else AttributeType.STRING

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
class ClientAuthorization(abc.ABC):
    client_id: uuid.UUID
    client_scopes: List[Scope]

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
            "type": "api_key",
            "client_id": str(self.client_id),
            "client_scopes": list(map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
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
        return ClientAuthorizationApiKey(uuid.UUID(d["client_id"]), client_scopes, d["api_key"],
                                         d["whitelist_hosts"] if "whitelist_hosts" in d else None)


@dataclass(frozen=True)
class ClientAuthorizationHttpSignature(ClientAuthorization):
    client_public_key: str

    def to_dict(self) -> dict:
        return {
            "type": "http_signature",
            "client_id": str(self.client_id),
            "client_scopes": list(map(lambda s: {"collection_name": s.collection_name, "scope_type": s.scope_type.name},
                                      self.client_scopes)),
            "public_key": self.client_public_key
        }

    @staticmethod
    def from_dict(d: dict) -> ClientAuthorizationHttpSignature:
        client_scopes = list(
            map(lambda c: Scope(c["collection_name"], ScopesType[c["scope_type"]]), d["client_scopes"]))
        return ClientAuthorizationHttpSignature(uuid.UUID(d["client_id"]), client_scopes, d["public_key"])


class ClientAuthorizationEntityAdapter(ClientAuthorizationEntity):
    def __init__(self, client_authorization: ClientAuthorization):
        super(ClientAuthorizationEntityAdapter, self).__init__(client_authorization.client_id,
                                                               client_authorization.to_dict())


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
        name = d["name"]
        configuration = d["configuration_name"]
        if "count" in d:
            return AggregationCount(name, configuration, d["count"])
        elif "sum" in d:
            return AggregationSum(name, configuration, d["sum"])
        elif "avg" in d:
            return AggregationAvg(name, configuration, d["avg"])
        else:
            raise ValueError('{} is not supported'.format(configuration))


@dataclass(frozen=True)
class AggregationCount(Aggregation):
    count: int

    def to_dict(self) -> dict:
        return {"name": self.name,
                "configuration_name": self.configuration_name,
                "count": self.count,
                "type": "COLLECTION_COUNT"}


@dataclass(frozen=True)
class AggregationSum(Aggregation):
    sum: float

    def to_dict(self) -> dict:
        return {"name": self.name, "configuration_name": self.configuration_name, "sum": self.sum, "type": "SUM"}


@dataclass(frozen=True)
class AggregationAvg(Aggregation):
    avg: float

    def to_dict(self) -> dict:
        return {"name": self.name, "configuration_name": self.configuration_name, "avg": Decimal(self.avg),
                "type": "AVG"}


@dataclass(frozen=True)
class AggregationConfiguration:
    id: uuid.UUID
    collection_name: str
    type: AggregationType
    on: List[AggregationTrigger]
    target_field: str
    matches: Predicate
    join: AggregationJoin

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
        inner_aggregation_document = document["aggregation"]
        on = list(map(lambda o: AggregationTrigger.value_of(o), inner_aggregation_document["on"]))
        target_field = None
        matches = None
        join = None
        if "target_field" in inner_aggregation_document:
            target_field = inner_aggregation_document["target_field"]
        if "join" in inner_aggregation_document:
            join = AggregationJoin(inner_aggregation_document["join"]["collection_name"],
                                   inner_aggregation_document["join"]["using_field"])
        if "matches" in inner_aggregation_document:
            matches = AggregationConfiguration.from_dict_to_predicate(inner_aggregation_document["matches"])

        uid = uuid.UUID(document["id"])
        return AggregationConfiguration(uid, collection_name, t, on, target_field, matches, join)

    def to_dict(self) -> dict:
        a = {
            "on": list(map(lambda o: o.name, self.on))
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
        d["id"] = self.id
        return d

    @staticmethod
    def from_predicate_to_dict(predicate: Predicate):
        if isinstance(predicate, Eq):
            return {"eq": {"field_name": predicate.field_name, "value": predicate.value}}
        elif isinstance(predicate, Range):
            return {
                "range": {"field_name": predicate.field_name, "from": predicate.from_value, "to": predicate.to_value}}
        elif isinstance(predicate, And):
            return {
                "and": list(map(lambda c: AggregationConfiguration.from_predicate_to_dict(c), predicate.conditions))}


class AggregationConfigurationEntityAdapter(AggregationConfigurationEntity):
    def __init__(self, aggregation_configuration: AggregationConfiguration):
        super(AggregationConfigurationEntity, self).__init__(aggregation_configuration.id,
                                                             aggregation_configuration.to_dict())


class CollectionService:

    def __init__(self):
        self.repo = repositories_v2.DynamoDBRepository('system', CollectionEntity)

    def get_collection(self, collection_name: str) -> Collection:
        result = self.repo.get(CollectionEntity(collection_name))
        if result:
            return Collection.from_dict(result.object())

    def create_collection(self, collection: Collection) -> Collection:
        result = self.repo.create(CollectionEntity(collection.name, collection.to_dict()))
        return Collection.from_dict(result.object())

    def delete_collection(self, collection_name: str) -> None:
        self.repo.delete(CollectionEntity(collection_name))

    def get_all_collections(self, limit: int = None, start_from: str = None) -> ([Collection], str):
        starting_from_collection = None
        if start_from:
            starting_from_collection = self.repo.get(CollectionEntity(start_from))
        result, last_evaluated_id = self.repo.query(QueryAll(CollectionEntity), limit, starting_from_collection)

        if result:
            return list(
                map(lambda m: Collection.from_dict(m.object()), result)), last_evaluated_id

    def get_all_collections_generator(self) -> Generator[Collection]:
        has_more = True
        while has_more:
            last_evaluated_key = None
            collections, last_evaluated_key = self.get_all_collections(start_from=last_evaluated_key)
            has_more = last_evaluated_key is not None
            for c in collections:
                yield c


class IndexService:

    def __init__(self):
        self.repo = repositories_v2.DynamoDBRepository('system', IndexEntity)

    def create_index(self, index: Index) -> Index:
        logger.debug("index : {}".format(index.__str__()))
        index_dict = Index.to_dict(index)
        logger.debug("index dict : {} ".format(index_dict))

        existing_index = self.get_index_by_collection_name_and_conditions(index.collection_name, index.conditions)

        if existing_index:
            return existing_index

        index_entity = IndexEntityAdapter(index)
        create_index_entity = self.repo.create(index_entity)
        logger.info("index created {}".format(create_index_entity.__str__()))
        if create_index_entity:
            created_index = Index.from_dict(create_index_entity.object())
            self.repo.indexing(IndexingOperation([],
                                                 [],
                                                 [
                                                     IndexByCollectionNameAndFieldsEntity(
                                                         index.id, index.collection_name,
                                                         index.conditions,
                                                         index_entity.object(),
                                                         index_entity.ordering()),
                                                     IndexByCollectionNameEntity(index.id, index.collection_name,
                                                                                 index_entity.object(),
                                                                                 index_entity.ordering())]))
            return created_index

    def get_index_by_collection_name_and_conditions(self, collection_name: str, fields: List[str]) -> Index | None:
        query = QueryIndexByCollectionNameAndFields(collection_name, fields)
        result, last_key = self.repo.query(query, 1, None)
        if result and len(result) == 1:
            return Index.from_dict(result[0].object())
        else:
            return None

    def get_index_by_collection_name(self, collection_name: str, limit: int = 20, start_from: uuid.UUID = None) -> (
            List[Index], uuid.UUID):
        last_index_entity = None
        if start_from:
            last_index_entity = self.__get_index_entity_by_id(start_from)
        data, last_evaluated_key = self.repo.query(QueryIndexByCollectionName(collection_name), limit,
                                                   last_index_entity)

        return list(
            map(lambda m: Index.from_dict(m.object()), data)), uuid.UUID(
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

    def __get_index_entity_by_id(self, uid: uuid.UUID) -> Model:
        return self.repo.get(IndexEntity(uid))


class AuthorizationService:

    def __init__(self):
        self.repo = repositories_v2.DynamoDBRepository('system', ClientAuthorizationEntity)

    def get_client_authorization(self, uid: uuid.UUID):
        result = self.repo.get(ClientAuthorizationEntity(uid))
        if result:
            return ClientAuthorization.from_dict(result.object())

    def create_client_authorization(self, client_authorization: ClientAuthorization):
        logging.info("creating client authorization {}".format(str(client_authorization)))

        model = self.repo.create(ClientAuthorizationEntityAdapter(client_authorization))
        if model:
            return client_authorization.from_dict(model.object())

    def update_authorization(self, client_authorization: ClientAuthorization):

        logging.info("updating client authorization {}".format(str(client_authorization)))

        model = self.repo.update(ClientAuthorizationEntityAdapter(client_authorization))
        if model:
            return ClientAuthorization.from_dict(model.object())

    def delete_authorization(self, client_id: str):
        logging.info("deleting client authorization {}".format(client_id))
        self.repo.delete(ClientAuthorizationEntity(client_id))


class AggregationService:

    def __init__(self):
        self.repo = repositories_v2.DynamoDBRepository('system', AggregationEntity)

    def get_aggregation_by_name(self, uid: uuid.UUID) -> Aggregation:

        result = self.__get_aggregation_entity_by_id(uid)
        if result:
            return Aggregation.from_dict(result.object())

    def __get_aggregation_entity_by_id(self, uid: uuid.UUID):
        return self.repo.get(AggregationEntity(uid))

    def get_aggregations_by_configuration_name(self, configuration_name: str, limit: int = 20,
                                               start_from: uuid.UUID = None) -> \
            Tuple[
                List[Union[Aggregation]], uuid.UUID]:
        start_from_entity = None
        if start_from:
            start_from_entity = self.__get_aggregation_entity_by_id(start_from)

        result, last_key = self.repo.query(QueryAggregationByAggregationConfigurationName(configuration_name), limit,
                                           start_from_entity)
        if result:
            return list(
                map(lambda m: Aggregation.from_dict(m.object()), result)), last_key

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
        result, last_key = self.repo.query(QueryAll(AggregationEntity), limit, start_from_entity)
        if result:
            return list(
                map(lambda m: Aggregation.from_dict(m.object()), result)), last_key

    def increment_count(self, aggregation: AggregationCount) -> Aggregation:
        entity = AggregationEntity(aggregation.id, aggregation.to_dict()).to_dynamo_db_item()

        self.repo.increment_counter(entity.to_dynamo_db_item(), [Counter("count", Decimal(1), True)])
        return AggregationCount(aggregation.id, aggregation.name, aggregation.configuration_name, aggregation.count + 1)

    def increment(self, aggregation: Aggregation, new_value: Decimal) -> Aggregation:

        entity = AggregationEntity(aggregation.id, aggregation.to_dict()).to_dynamo_db_item()
        counter, converter = AggregationService.__get_increment(aggregation, new_value)

        result = self.repo.increment_counter(entity.to_dynamo_db_item(), [counter])
        if result:
            return converter(aggregation)
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
                                 int(delta.to_integral()))
        elif isinstance(aggregation, AggregationSum):
            delta = new_value - aggregation.sum
            return Counter('sum', delta, True if delta >= 0 else False), lambda a: \
                AggregationSum(aggregation.id,
                               aggregation.name,
                               aggregation.configuration_name,
                               float(delta))
        elif isinstance(aggregation, AggregationAvg):
            delta = new_value - Decimal(aggregation.avg)
            return Counter('avg', delta, True if delta >= 0 else False), lambda a: \
                AggregationAvg(aggregation.id,
                               aggregation.name,
                               aggregation.configuration_name,
                               float(delta))
        else:
            raise ValueError("invalid aggregation {}".format(aggregation))

    def decrement_count(self, aggregation: AggregationCount) -> Aggregation:
        entity = AggregationEntity(aggregation.id, aggregation.to_dict()).to_dynamo_db_item()

        self.repo.increment_counter(entity.to_dynamo_db_item(), [Counter("count", Decimal(1), False)])
        return AggregationCount(aggregation.id, aggregation.name, aggregation.configuration_name, aggregation.count - 1)

    def create_aggregation(self, aggregation: Aggregation) -> Aggregation:
        created_entity = self.repo.create(AggregationEntity(aggregation.id, aggregation.to_dict()))
        return Aggregation.from_dict(created_entity.object())

    def update_aggregation(self, aggregation: Aggregation) -> Aggregation:
        updated_entity = self.repo.update(AggregationEntity(aggregation.id, aggregation.to_dict()))
        return Aggregation.from_dict(updated_entity.object())


class AggregationConfigurationService:

    def __from_entity_to_aggregation_configuration(m: Model):
        return AggregationConfiguration.from_dict_to_aggregation_configuration(m.object())

    def __init__(self):
        self.repo = repositories_v2.DynamoDBRepository('system', AggregationConfigurationEntity)

    def __get_aggregation_configuration_entity_by_id(self, uid: uuid.UUID) -> Model:
        return self.repo.get(AggregationConfigurationEntity(uid))

    def get_aggregation_by_uid(self, uid: uuid.UUID):
        entity = self.__get_aggregation_configuration_entity_by_id(uid)
        return AggregationConfigurationService.__from_entity_to_aggregation_configuration(entity)

    def get_all_aggregation_configurations(self, limit: int, start_from: uuid.UUID) -> \
            (List[AggregationConfiguration], uuid.UUID):
        start_after_entity = None
        if start_from:
            start_after_entity = self.__get_aggregation_configuration_entity_by_id(start_from)
        result, last_evaluated_key = self.repo.query(QueryAll(AggregationEntity), limit, start_after_entity)
        if result:
            return list(map(lambda m: AggregationConfigurationService.__from_entity_to_aggregation_configuration(),
                            result)), uuid.UUID(last_evaluated_key) if last_evaluated_key is not None else None

    def create_aggregation_configuration(self, aggregation_configuration: AggregationConfiguration):
        created_entity = self.repo.create(AggregationConfigurationEntityAdapter(aggregation_configuration))
        return AggregationConfigurationService.__from_entity_to_aggregation_configuration(created_entity)

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
        result, last_evaluated_key = self.repo.query(QueryAggregationConfigurationByCollectionName(collection_name),
                                                     limit, start_after_entity)
        if result:
            return list(map(lambda m: AggregationConfigurationService.__from_entity_to_aggregation_configuration(),
                            result)), uuid.UUID(last_evaluated_key) if last_evaluated_key is not None else None
