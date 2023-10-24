from typing import Type, List

from aws.dynamodb.dynamodbdao import DynamoDBDAO, DynamoDBKey, AtomicIncrement, Counter as DynamoDBCounter, \
    DynamoDBModel, DynamoDBQuery, FIELD_SEPARATOR, GSIDynamoDBQuery
from dynamoplus.models.query.conditions import Predicate, AnyMatch, And, Range
from dynamoplus.v2.repository.repositories_v2 import RepositoryInterface, Model, Query, Counter, IndexingOperation, \
    IndexModel, EqCondition, GtCondition, GteCondition, LteCondition, LtCondition, Condition, AndCondition, \
    AnyCondition, BeginsWithCondition, BetweenCondition, CounterIncrement


class DynamoDBRepository(RepositoryInterface):

    def __init__(self, table_name: str):
        self.dao = DynamoDBDAO(table_name)

    def create(self, model: Model) -> dict:
        result = self.dao.create(convert_model_to_dynamo_db_model(model))
        return result.document

    def update(self, model: Model) -> dict:
        result = self.dao.update(convert_model_to_dynamo_db_model(model))
        return result.document

    def get(self, id: str, entity_name: str) -> dict:
        pk, sk = self.__get_pk_sk(entity_name, id)
        result = self.dao.get(pk, sk)
        if result:
            return result.document
        else:
            return None

    def __get_pk_sk(self, entity_name, id):
        pk = entity_name + FIELD_SEPARATOR + id
        sk = entity_name
        return pk, sk

    def delete(self, id: str, entity_name: str) -> None:
        pk, sk = self.__get_pk_sk(entity_name, id)
        self.dao.delete(pk, sk)

    def indexing(self, indexing: IndexingOperation) -> None:

        for r in indexing.to_delete:
            pk = r.entity_name() + FIELD_SEPARATOR + r.id()
            sk = r.index_name()
            self.dao.delete(pk, sk)
        for r in indexing.to_update:
            self.dao.update(DynamoDBModel(r.entity_name() + FIELD_SEPARATOR + r.id(),
                                          r.index_name(),
                                          r.index_value(),
                                          r.document()))
        for r in indexing.to_create:
            self.dao.create(DynamoDBModel(r.entity_name() + FIELD_SEPARATOR + r.id(),
                                          r.index_name(),
                                          r.index_value(),
                                          r.document()))

    def query(self, entity_name: str, condition: Condition, limit: int, starting_after: str = None) -> (
            List[dict], str):
        start_from = get_starting_from(starting_after)

        dynamo_db_query = build_dynamo_query(condition, entity_name)

        result = self.dao.query(dynamo_db_query, limit, start_from)
        last_evaluated_model = None
        if result.lastEvaluatedKey:
            last_evaluated_model = from_dynamo_db_key(result.lastEvaluatedKey, entity_name)

        return list(map(lambda d: self.handle_optimized_write_result(d, entity_name),
                        result.data)), last_evaluated_model

    def handle_optimized_write_result(self, dynamo_db_model: DynamoDBModel, entity_name: str):
        if dynamo_db_model.document is None:
            return self.get(dynamo_db_model.pk.replace(entity_name + "#", ''), entity_name)
        else:
            return dynamo_db_model.document

    def increment_count(self, param: CounterIncrement):
        self.dao.increment_counter(AtomicIncrement(param.entity_name() + "#" + param.id, param.entity_name(),
                                                   [DynamoDBCounter(
                                                       "document." + param.field_name,
                                                       param.increment,
                                                       param.increment > 0
                                                   )]))

    def increment_counter(self, model: Model, counters: List[Counter]):

        dynamodb_counters = list(map(convert_counter_to_dynamodb, counters))
        db_model = convert_model_to_dynamo_db_model(model)
        self.dao.increment_counter(
            AtomicIncrement(db_model.pk, db_model.sk, dynamodb_counters))

    def create_table(self):
        self.dao.create_table()

    def cleanup_table(self):
        self.dao.cleanup_table()


def convert_counter_to_dynamodb(counter: Counter) -> DynamoDBCounter:
    return DynamoDBCounter(
        counter.field_name,
        counter.count,
        counter.is_increment
    )


def convert_index_model_to_dynamo_db_model(index_model: IndexModel) -> DynamoDBModel:
    pk = index_model.entity_name() + "#" + index_model.id()
    sk = index_model.index_name()
    data = index_model.index_value()
    document = index_model.document()
    return DynamoDBModel(pk, sk, data, document)


def convert_model_to_dynamo_db_model(model: Model):
    pk = model.entity_name() + "#" + model.id()
    sk = model.entity_name()
    data = model.ordering() if model.ordering() else model.id()
    document = model.object()
    return DynamoDBModel(pk, sk, data, document)


def handle_eq_condition(dynamo_db_query: GSIDynamoDBQuery, predicate: EqCondition, collection_name: str,
                        fields: List[str], values: List[str]) -> object:
    # collection_name#field1#field2#eq
    # value1#value2#eqvalue
    separator = FIELD_SEPARATOR if len(fields) > 0 else ""
    pk = collection_name + FIELD_SEPARATOR + FIELD_SEPARATOR.join(fields) + separator + predicate.field_name
    sk = FIELD_SEPARATOR.join(values) + separator + predicate.field_value
    dynamo_db_query.eq(pk, sk)
    return dynamo_db_query


def handle_gt_condition(dynamo_db_query: GSIDynamoDBQuery, predicate: GtCondition, collection_name: str,
                        fields: List[str], values: List[str]):
    # collection_name#field1#field2#eq
    # value1#value2#eqvalue
    separator = FIELD_SEPARATOR if len(fields) > 0 else ""
    pk = collection_name + FIELD_SEPARATOR + FIELD_SEPARATOR.join(fields) + separator + predicate.field_name
    sk = FIELD_SEPARATOR.join(values) + separator + predicate.field_value
    dynamo_db_query.gt(pk, sk)
    return dynamo_db_query


def handle_gte_condition(dynamo_db_query: GSIDynamoDBQuery, predicate: GteCondition, collection_name: str,
                         fields: List[str], values: List[str]):
    # collection_name#field1#field2#eq
    # value1#value2#eqvalue
    separator = FIELD_SEPARATOR if len(fields) > 0 else ""
    pk = collection_name + FIELD_SEPARATOR + FIELD_SEPARATOR.join(fields) + separator + predicate.field_name
    sk = FIELD_SEPARATOR.join(values) + separator + predicate.field_value
    dynamo_db_query.gte(pk, sk)
    return dynamo_db_query


def handle_lt_condition(dynamo_db_query: GSIDynamoDBQuery, predicate: LtCondition, collection_name: str,
                        fields: List[str], values: List[str]):
    # collection_name#field1#field2#eq
    # value1#value2#eqvalue
    separator = FIELD_SEPARATOR if len(fields) > 0 else ""
    pk = collection_name + FIELD_SEPARATOR + FIELD_SEPARATOR.join(fields) + separator + predicate.field_name
    sk = FIELD_SEPARATOR.join(values) + separator + predicate.field_value
    dynamo_db_query.lt(pk, sk)
    return dynamo_db_query


def handle_lte_condition(dynamo_db_query: GSIDynamoDBQuery, predicate: LteCondition, collection_name: str,
                         fields: List[str], values: List[str]):
    # collection_name#field1#field2#eq
    # value1#value2#eqvalue
    separator = FIELD_SEPARATOR if len(fields) > 0 else ""
    pk = collection_name + FIELD_SEPARATOR + "__".join(fields) + "__" + predicate.field_name
    sk = FIELD_SEPARATOR.join(values) + separator + predicate.field_value
    dynamo_db_query.lte(pk, sk)
    return dynamo_db_query


def handle_range_condition(dynamo_db_query: GSIDynamoDBQuery, predicate: BetweenCondition, collection_name: str,
                           fields: List[str], values: List[str]):
    # collection_name#field1#field2#eq
    # value1#value2#eqvalue
    separator = FIELD_SEPARATOR if len(fields) > 0 else ""
    pk = collection_name + FIELD_SEPARATOR + "__".join(fields) + "__" + predicate.field_name
    sk_from = FIELD_SEPARATOR.join(values) + separator + predicate.field_value_from
    sk_to = FIELD_SEPARATOR.join(values) + separator + predicate.field_value_to
    dynamo_db_query.between(pk, sk_from, sk_to)
    return dynamo_db_query


def handle_begins_with_condition(dynamo_db_query: GSIDynamoDBQuery, predicate: BeginsWithCondition,
                                 collection_name: str,
                                 fields: List[str], values: List[str]):
    # collection_name#field1#field2#eq
    # value1#value2#eqvalue
    separator = FIELD_SEPARATOR if len(fields) > 0 else ""
    x = ""
    if len(fields) > 0:
        x = "__".join(fields) + "__"
    pk = collection_name + FIELD_SEPARATOR + x + predicate.field_name
    sk = FIELD_SEPARATOR.join(values) + separator + predicate.field_value
    dynamo_db_query.begins_with(pk, sk)
    return dynamo_db_query


def build_dynamo_query(predicate: Condition, collection_name: str) -> DynamoDBQuery:
    dynamo_db_query = GSIDynamoDBQuery()

    predicate_type_mapping = {

        EqCondition: lambda fields, values, eq_condition: handle_eq_condition(dynamo_db_query, eq_condition,
                                                                              collection_name, fields,
                                                                              values),
        GtCondition: lambda fields, values, gt_condition: handle_gt_condition(dynamo_db_query, gt_condition,
                                                                              collection_name, fields,
                                                                              values),
        GteCondition: lambda fields, values, gte_condition: handle_gte_condition(dynamo_db_query, gte_condition,
                                                                                 collection_name, fields,
                                                                                 values),
        LteCondition: lambda fields, values, lte_condition: handle_lte_condition(dynamo_db_query, lte_condition,
                                                                                 collection_name, fields,
                                                                                 values),
        LtCondition: lambda fields, values, lt_condition: handle_lt_condition(dynamo_db_query, lt_condition,
                                                                              collection_name, fields,
                                                                              values),
        BetweenCondition: lambda fields, values, range_condition: handle_range_condition(dynamo_db_query,
                                                                                         range_condition,
                                                                                         collection_name, fields,
                                                                                         values),
        BeginsWithCondition: lambda fields, values, begins_with_condition: handle_begins_with_condition(dynamo_db_query,
                                                                                                        begins_with_condition,
                                                                                                        collection_name,
                                                                                                        fields,
                                                                                                        values)
    }

    if isinstance(predicate, AnyCondition):
        return dynamo_db_query.all(collection_name)
    elif isinstance(predicate, AndCondition):
        fields = []
        prefix_values = []
        for eq in predicate.eq_conditions:
            fields.append(eq.field_name)
            prefix_values.append(eq.field_value)
        if predicate.last_condition:
            # fields.append(predicate.last_condition.field_name)
            handler = predicate_type_mapping.get(type(predicate.last_condition))
            return handler(fields, prefix_values, predicate.last_condition)
        else:
            pk = collection_name + FIELD_SEPARATOR + FIELD_SEPARATOR.join(fields)
            sk = FIELD_SEPARATOR.join(prefix_values)
            return dynamo_db_query.eq(pk, sk)

    else:
        handler = predicate_type_mapping.get(type(predicate))
        return handler([], [], predicate)


# dynamo_db_query = GSIDynamoDBQuery()
#         field_names, field_values = self.__extract_fields_eq()
#         if self._lt:
#             field_names.append(self._lt.field_name)
#             field_values.append(self._lt.field_value)
#             partition_key, sort_key = self.__build_keys(field_names, field_values)
#             dynamo_db_query.lt(self.collection_name + "#" + partition_key, sort_key)
#         elif self._gt:
#             field_names.append(self._gt.field_name)
#             field_values.append(self._gt.field_value)
#             partition_key, sort_key = self.__build_keys(field_names, field_values)
#             dynamo_db_query.gt(self.collection_name + "#" + partition_key, sort_key)
#         elif self._lte:
#             field_names.append(self._lte.field_name)
#             field_values.append(self._lte.field_value)
#             partition_key, sort_key = self.__build_keys(field_names, field_values)
#             dynamo_db_query.lte(self.collection_name + "#" + partition_key, sort_key)
#         elif self._gte:
#             field_names.append(self._gte.field_name)
#             field_values.append(self._gte.field_value)
#             partition_key, sort_key = self.__build_keys(field_names, field_values)
#             dynamo_db_query.gte(self.collection_name + "#" + partition_key, sort_key)
#         elif self._begins_with:
#             field_names.append(self._begins_with.field_name)
#             field_values.append(self._begins_with.field_value)
#             partition_key, sort_key = self.__build_keys(field_names, field_values)
#             dynamo_db_query.begins_with(self.collection_name + "#" + partition_key, sort_key)
#         elif self._between:
#             field_names.append(self._between.field_name)
#             partition_key, sort_key = self.__build_keys(field_names, field_values)
#             sort_key_from = sort_key + FIELD_SEPARATOR + self._between.field_value_from
#             sort_key_to = sort_key + FIELD_SEPARATOR + self._between.field_value_to
#             dynamo_db_query.between(self.collection_name + "#" + partition_key, sort_key_from, sort_key_to)
#         else:
#             if len(field_names) != 0:
#                 partition_key, sort_key = self.__build_keys(field_names, field_values)
#                 dynamo_db_query.eq(self.collection_name + "#" + partition_key, sort_key)
#             else:
#                 dynamo_db_query.all(self.collection_name)


def __build_keys(field_names, field_values):
    partition_key = FIELD_SEPARATOR.join(field_names)
    sort_key = FIELD_SEPARATOR.join(field_values)
    return partition_key, sort_key


def __extract_fields_eq(self):
    field_names = []
    field_values = []
    if self._eq:
        for eq_condition in self._eq:
            field_names.append(eq_condition.field_name)
            field_values.append(eq_condition.field_value)
    return field_names, field_values


def from_dynamo_db_key(last_evaluated_key: DynamoDBKey, collection_name: str) -> str:
    return last_evaluated_key.partition_key.replace(collection_name, '').replace(FIELD_SEPARATOR, '')


def get_starting_from(starting_after):
    start_from = None
    if starting_after:
        starting_after_dynamo_db_model = convert_model_to_dynamo_db_model(starting_after)
        start_from = DynamoDBKey(starting_after_dynamo_db_model.pk, starting_after_dynamo_db_model.sk,
                                 starting_after_dynamo_db_model.data)
    return start_from
