from typing import *

from dynamoplus.models.query.conditions import Predicate
from dynamoplus.v2.service.system.system_service_v2 import Index, Collection


def find_sk_query(collection: Collection, fields: List[str]) -> str:
    return collection.name + (
        "#{}".format("#".join(fields)) if fields is not None and len(fields) > 0 else "")


class QueryService:

    @staticmethod
    def query_generator(collection: Collection, predicate: Predicate, index: Index):
        has_more = True
        start_from = None
        while has_more:
            try:
                query_result = QueryService.__query(collection,predicate,index, start_from, 20)
                if query_result and query_result.data and len(query_result.data)>0:
                    has_more = query_result.lastEvaluatedKey is not None

                    for d in query_result.data:
                        yield d
                    start_from = query_result.lastEvaluatedKey
                else:
                    has_more = False
            except StopIteration:
                break



    @staticmethod
    def query(collection: Collection, predicate: Predicate, index: Index, start_from: str = None,
              limit: int = 20) -> QueryResult:
        result = None
        if predicate.is_range():
            result = QueryService.__query_range(collection, predicate, index.conditions, start_from, limit)
        elif isinstance(predicate, AnyMatch):
            result = QueryService.__query_all(collection, limit, start_from)
        else:
            result = QueryService.__query_begins_with(collection, predicate, index.conditions, start_from, limit)
        if index is not None and index.index_configuration == IndexConfiguration.OPTIMIZE_WRITE:
            result = QueryResult(list(map(lambda m: Repository(get_table_name(is_system(collection))).get(m.pk, collection.name), result.data)), result.lastEvaluatedKey)
        return result

    @staticmethod
    def __query(collection: Collection, predicate: Predicate, index: Index, start_from: Model = None,
              limit: int = 20) -> QueryResult:
        result = None
        if predicate.is_range():
            result = QueryService.__query_range_starting_after_model(collection, predicate, index.conditions, start_from, limit)
        elif isinstance(predicate, AnyMatch):
            result = QueryService.__query_all_starting_after_model(collection, limit, start_from)
        else:
            result = QueryService.__query_begins_with_starting_after_model(collection, predicate, index.conditions, start_from, limit)
        if index is not None and index.index_configuration == IndexConfiguration.OPTIMIZE_WRITE:
            result = QueryResult(list(
                map(lambda m: Repository(get_table_name(is_system(collection))).get(m.pk, collection.name),
                    result.data)), result.lastEvaluatedKey)
        return result

    @staticmethod
    def __query_range(collection: Collection, predicate: Predicate, fields: List[str], start_from: str = None,
                      limit: int = 20) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        range_predicate = get_range_predicate(predicate)
        sk = find_sk_query(collection, fields)
        data_prefix = ""
        non_range_values = list(
            filter(lambda v: range_predicate.to_value != v and range_predicate.from_value != v, predicate.get_values()))
        if len(non_range_values) > 0:
            data_prefix = "#".join(non_range_values)
            data_prefix = data_prefix + "#"
        data1 = data_prefix + range_predicate.from_value
        data2 = data_prefix + range_predicate.to_value
        repo = QueryRepository(table_name)
        last_evaluated_item = None
        if start_from:
            last_evaluated_item = Repository(table_name).get(get_pk(collection, start_from), sk)
        return repo.query_range(sk, data1, data2, limit, last_evaluated_item)

    @staticmethod
    def __query_range_starting_after_model(collection: Collection, predicate: Predicate, fields: List[str], last_evaluated_item: Model = None,
                      limit: int = 20) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        range_predicate = get_range_predicate(predicate)
        sk = find_sk_query(collection, fields)
        data_prefix = ""
        non_range_values = list(
            filter(lambda v: range_predicate.to_value != v and range_predicate.from_value != v, predicate.get_values()))
        if len(non_range_values) > 0:
            data_prefix = "#".join(non_range_values)
            data_prefix = data_prefix + "#"
        data1 = data_prefix + range_predicate.from_value
        data2 = data_prefix + range_predicate.to_value
        repo = QueryRepository(table_name)
        return repo.query_range(sk, data1, data2, limit, last_evaluated_item)

    @staticmethod
    def __query_begins_with(collection: Collection, predicate: Predicate, fields: List[str], start_from: str = None,
                            limit: int = 20) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        data = "#".join(predicate.get_values())
        sk = find_sk_query(collection, fields)
        repo = QueryRepository(table_name)
        last_evaluated_item = None
        if start_from:
            ## should start from last index row
            last_evaluated_item = Repository(table_name).get(get_pk(collection, start_from), sk)
        return repo.query_begins_with(sk, data, last_evaluated_item, limit)

    @staticmethod
    def __query_begins_with_starting_after_model(collection: Collection, predicate: Predicate, fields: List[str], last_evaluated_item: Model = None,
                            limit: int = 20) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        data = "#".join(predicate.get_values())
        sk = find_sk_query(collection, fields)
        repo = QueryRepository(table_name)
        return repo.query_begins_with(sk, data, last_evaluated_item, limit)

    @staticmethod
    def __query_all(collection: Collection, limit: int, start_from: str = None) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        repo = QueryRepository(table_name)
        last_evaluated_item = None
        if start_from:
            ## this is the main row, not the index, since there is no index
            last_evaluated_item = Repository(table_name).get(get_pk(collection, start_from), get_sk(collection))
        sk = find_sk_query(collection, [])
        return repo.query_all(sk, last_evaluated_item, limit)

    @staticmethod
    def __query_all_starting_after_model(collection: Collection, limit: int, last_evaluated_item: Model = None) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        repo = QueryRepository(table_name)
        sk = find_sk_query(collection, [])
        res = repo.query_all(sk, last_evaluated_item, limit)
        return res
