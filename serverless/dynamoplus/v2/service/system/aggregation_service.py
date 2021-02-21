from _pydecimal import Decimal

from dynamoplus.models.query.conditions import match_predicate
from dynamoplus.v2.repository.repositories import Counter
from dynamoplus.models.system.aggregation.aggregation import Aggregation, AggregationType
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.models.system.index.index import Index
from dynamoplus.v2.repository.repositories import Repository, AtomicIncrement
from dynamoplus.v2.service.common import get_repository_factory
from dynamoplus.v2.service.domain.domain_service import DomainService
from dynamoplus.v2.service.model_service import get_model, get_index_model
from dynamoplus.v2.service.system.system_service import Converter, aggregation_metadata, logger, CollectionService, \
    collection_metadata


def extract_sum_and_count(aggregation: Aggregation, new_record, old_record):
    counters = []
    if new_record and old_record:
        ## update
        if aggregation.target_field in old_record and aggregation.target_field in new_record:
            old_value = old_record[aggregation.target_field]
            new_value = new_record[aggregation.target_field]
            increase = new_value - old_value
            counters = [
                Counter("{}_sum".format(aggregation.target_field),
                        Decimal(abs(increase)), increase > 0)
            ]
    elif new_record is not None and old_record is None:
        ##insert
        if aggregation.target_field in new_record:
            counters = [
                Counter("{}_count".format(aggregation.target_field), Decimal(1)),
                Counter("{}_sum".format(aggregation.target_field), Decimal(new_record[aggregation.target_field]))
            ]
    elif old_record is not None:
        ## delete
        if aggregation.target_field in old_record:
            counters = [
                Counter("{}_count".format(aggregation.target_field), Decimal(1), False),
                Counter("{}_sum".format(aggregation.target_field),
                        Decimal(old_record[aggregation.target_field]), False)
            ]
    if aggregation.target_field is None:
        is_increment = True if new_record is not None else False
        counters = counters + [Counter("count", Decimal(1), is_increment)]
    return counters


class AggregationProcessingService:

    def aggregate(aggregation: Aggregation, collection: Collection, new_record: dict, old_record: dict):
        r = new_record or old_record
        target_collection = collection_metadata
        join_document = None
        if aggregation.join:
            if aggregation.join.collection_name:
                target_collection = CollectionService.get_collection(aggregation.join.collection_name)
            if aggregation.join.using_field in r:
                id = r[aggregation.join.using_field]
                join_document = DomainService(target_collection).get_document(id)

        repo = Repository(get_repository_factory(target_collection))

        counters = extract_sum_and_count(aggregation, new_record, old_record)

        if len(counters) > 0:
            if join_document:
                model = get_model(target_collection, join_document)
            else:
                model = get_model(collection_metadata, {"name": aggregation.collection_name})
            repo.increment_counter(AtomicIncrement(model.pk,
                                                   model.sk,
                                                   counters))

    def avg(aggregation: Aggregation, collection: Collection, new_record: dict, old_record: dict):
        repo = Repository(get_repository_factory(collection_metadata))
        model = get_model(collection_metadata, {"name": aggregation.collection_name})
        counters = extract_sum_and_count(aggregation, new_record, old_record)

        if len(counters) > 0:
            repo.increment_counter(AtomicIncrement(model.pk,
                                                   model.sk,
                                                   counters))

    def collection_count(aggregation: Aggregation, collection: Collection, new_record: dict, old_record: dict):
        repo = Repository(get_repository_factory(collection_metadata))
        model = get_model(collection_metadata, {"name": aggregation.collection_name})

        is_increment = True if new_record is not None else False
        increment = AtomicIncrement(model.pk, model.sk, [Counter("count", Decimal(1), is_increment)])
        repo.increment_counter(increment)

    # def avg_join(aggregation: Aggregation, collection: Collection, new_record: dict, old_record: dict):
    #     repo = Repository(get_repository_factory(collection))
    #
    #     target_collection = CollectionService.get_collection(aggregation.join.collection_name)
    #     r = new_record or old_record
    #     if aggregation.join.using_field in r:
    #         id = r[aggregation.join.using_field]
    #         join_document = DomainService(target_collection).get_document(id)
    #
    #     counters = extract_sum_and_count(aggregation, new_record, old_record)
    #
    #     if len(counters) > 0 and join_document:
    #         model = get_model(target_collection, join_document)
    #         repo.increment_counter(AtomicIncrement(model.pk,
    #                                                model.sk,
    #                                                counters))
    #
    #     return None

    def max(aggregation: Aggregation, document: dict):
        return None

    def min(aggregation: Aggregation, document: dict):
        return None

    # def sum(aggregation: Aggregation, collection: Collection, new_record: dict, old_record: dict):
    #     repo = Repository(get_repository_factory(collection))
    #
    #     target_collection = CollectionService.get_collection(aggregation.join.collection_name)
    #     r = new_record or old_record
    #     if aggregation.join.using_field in r:
    #         id = r[aggregation.join.using_field]
    #         #join_document = DomainService(target_collection).get_document(id)
    #
    #     counters = extract_sum_and_count(aggregation, new_record, old_record)
    #
    #     if len(counters) > 0 and join_document:
    #         model = get_model(target_collection, join_document)
    #         repo.increment_counter(AtomicIncrement(model.pk,
    #                                                model.sk,
    #                                                counters))

    aggregation_executor_factory = {
        AggregationType.AVG: aggregate,
        AggregationType.COLLECTION_COUNT: aggregate,
        AggregationType.AVG_JOIN: aggregate,
        AggregationType.MAX: max,
        AggregationType.MIN: min,
        AggregationType.SUM: aggregate,
        AggregationType.SUM_COUNT: aggregate
    }

    @staticmethod
    def execute_aggregation(aggregation: Aggregation, collection: Collection, new_record: dict, old_record: dict):
        if aggregation.matches:
            if not match_predicate(new_record, aggregation.matches):
                return
        AggregationProcessingService.aggregation_executor_factory[aggregation.type](aggregation, collection, new_record,
                                                                                    old_record)
