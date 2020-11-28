from typing import *
from dynamoplus.models.query.conditions import Predicate, get_range_predicate, AnyMatch
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index
from dynamoplus.repository.models import QueryModel
from dynamoplus.v2.repository.repositories import QueryResult, get_table_name, QueryRepository, Repository
from dynamoplus.v2.service.model_service import get_pk, get_sk
from dynamoplus.v2.service.common import is_system


def find_sk_query(collection: Collection, fields: List[str]) -> str:
    return collection.name + (
        "#{}".format("#".join(fields)) if fields is not None and len(fields) > 0 else "")


class QueryService:

    @staticmethod
    def query(collection: Collection, predicate: Predicate, index: Index, start_from: str = None,
              limit: int = 20) -> QueryResult:
        if predicate.is_range():
            return QueryService.__query_range(collection, predicate, index.conditions, start_from, limit)
        elif isinstance(predicate, AnyMatch):
            return QueryService.__query_all(collection, limit, start_from)
        else:
            return QueryService.__query_begins_with(collection, predicate, index.conditions, start_from, limit)

    @staticmethod
    def __query_range(collection: Collection, predicate: Predicate, fields: List[str], start_from: str = None,
                      limit: int = 20) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        range_predicate = get_range_predicate(predicate)
        data_prefix = ""
        non_range_values = list(
            filter(lambda v: range_predicate.to_value != v and range_predicate.from_value != v, predicate.get_values()))
        if len(non_range_values) > 0:
            data_prefix = "#".join(non_range_values)
            data_prefix = data_prefix + "#"
        data1 = data_prefix + range_predicate.from_value
        data2 = data_prefix + range_predicate.to_value
        query_model = QueryModel(collection, fields, predicate)
        repo = QueryRepository(table_name)
        last_evaluated_item = None
        if start_from:
            last_evaluated_item = Repository(table_name).get(get_pk(collection, start_from), get_sk(collection))
        return repo.query_range(query_model.sk(), data1, data2, limit, last_evaluated_item)

    @staticmethod
    def __query_begins_with(collection: Collection, predicate: Predicate, fields: List[str], start_from: str = None,
                            limit: int = 20) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        data = "#".join(predicate.get_values())
        sk = find_sk_query(collection, fields)
        repo = QueryRepository(table_name)
        last_evaluated_item = None
        if start_from:
            last_evaluated_item = Repository(table_name).get(get_pk(collection, start_from), get_sk(collection))
        return repo.query_begins_with(sk, data, last_evaluated_item, limit)

    @staticmethod
    def __query_all(collection: Collection, limit: int, start_from: str = None) -> QueryResult:
        table_name = get_table_name(is_system(collection))
        repo = QueryRepository(table_name)
        last_evaluated_item = None
        if start_from:
            last_evaluated_item = Repository(table_name).get(get_pk(collection, start_from), get_sk(collection))
        sk = find_sk_query(collection, [])
        return repo.query_all(sk, last_evaluated_item, limit)
