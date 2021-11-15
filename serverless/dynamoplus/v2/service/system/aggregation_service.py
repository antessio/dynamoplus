import logging
from _pydecimal import Decimal
from typing import List

from dynamoplus.models.query.conditions import match_predicate
from dynamoplus.models.system.aggregation.aggregation import AggregationConfiguration, AggregationType, Aggregation, \
    AggregationCount
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.v2.repository.repositories import Counter
from dynamoplus.v2.repository.repositories import Repository, AtomicIncrement
from dynamoplus.v2.service.common import get_repository_factory

from dynamoplus.v2.service.model_service import get_model
from dynamoplus.v2.service.system.system_service import AggregationConfigurationService,\
    AggregationService

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# TODO
#
# - for MAX and MIN: an index by the field must be present so if it doesn't exist, then create it and make it unmodifiable
#   - in the API or whatever may change the index, the update must be forbidden
# - for count and sum: they are strictly connected, if avg is defined then sum is automatically created
#

def extract_sum_and_count(aggregation: AggregationConfiguration, new_record, old_record):
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

    # def aggregate(aggregation_configuration: AggregationConfiguration, collection: Collection, new_record: dict, old_record: dict):
    #     record = new_record or old_record
    #
    #     aggregation = AggregationService.get_aggregation_by_name(aggregation_configuration.name)
    #
    #     count = aggregation.count or 0
    #     max = aggregation.max or [0.0]
    #     avg = aggregation.avg or 0.0
    #
    #
    #
    #     counters = extract_sum_and_count(aggregation_configuration, new_record, old_record)
    #
    #     if len(counters) > 0:
    #             model = get_model(collection_metadata, {"name": aggregation_configuration.collection_name})
    #
    #     repo.increment_counter(AtomicIncrement(model.pk,
    #                                                model.sk,
    #                                                counters))
    #
    # def avg(aggregation: AggregationConfiguration, collection: Collection, new_record: dict, old_record: dict):
    #     repo = Repository(get_repository_factory(collection_metadata))
    #     model = get_model(collection_metadata, {"name": aggregation.collection_name})
    #     counters = extract_sum_and_count(aggregation, new_record, old_record)
    #
    #     if len(counters) > 0:
    #         repo.increment_counter(AtomicIncrement(model.pk,
    #                                                model.sk,
    #                                                counters))

    def collection_count(aggregation_configuration: AggregationConfiguration,
                         collection: Collection,
                         new_record: dict,
                         old_record: dict):
        ## load aggregation
        aggregation = AggregationService.get_aggregation_by_name(aggregation_configuration.name)

        if aggregation:
            if isinstance(aggregation, AggregationCount):
                is_increment = True if old_record is None else False
                is_decrement = True if new_record is None else False
                if is_increment:
                    if AggregationService.incrementCount(aggregation):
                        aggregation.count = aggregation.count + 1

                elif is_decrement:
                    if AggregationService.decrementCount(aggregation):
                        aggregation.count = aggregation.count - 1
            return aggregation

        else:
            ## create the aggregation if it doesn't exist
            logger.info("found aggregation configuration {}".format(aggregation_configuration))
            return AggregationService.createAggregation(AggregationCount(aggregation_configuration.name,1))


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

    def max(aggregation: AggregationConfiguration, old_document: dict,new_document:dict):
        ## create "normal" index by field ordered by itself
        return None

    def min(aggregation: AggregationConfiguration, old_document: dict,new_document:dict):
        ## create "normal" index by field order by itself
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
        # AggregationType.AVG: aggregate,
        AggregationType.COLLECTION_COUNT: collection_count,
        #AggregationType.AVG_JOIN: aggregate,
        # AggregationType.MAX: max,
        # AggregationType.MIN: min,
        # AggregationType.SUM: aggregate,
        #AggregationType.SUM_COUNT: aggregate
    }

    @staticmethod
    def execute_aggregation(aggregation: AggregationConfiguration, collection: Collection, new_record: dict, old_record: dict):
        if aggregation.matches:
            if not match_predicate(new_record, aggregation.matches):
                return
        AggregationProcessingService.aggregation_executor_factory[aggregation.type](aggregation, collection, new_record, old_record)
