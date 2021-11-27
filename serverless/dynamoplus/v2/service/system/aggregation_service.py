import logging
from _pydecimal import Decimal
from typing import List

from dynamoplus.models.query.conditions import match_predicate
from dynamoplus.models.system.aggregation.aggregation import AggregationConfiguration, AggregationType, Aggregation, \
    AggregationCount, AggregationTrigger, AggregationSum, AggregationAvg
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.v2.repository.repositories import Counter
from dynamoplus.v2.repository.repositories import Repository, AtomicIncrement
from dynamoplus.v2.service.common import get_repository_factory

from dynamoplus.v2.service.model_service import get_model
from dynamoplus.v2.service.system.system_service import AggregationConfigurationService, \
    AggregationService

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# TODO
#
# - for MAX and MIN: an index by the field must be present so if it doesn't exist, then create it and make it unmodifiable
#   - in the API or whatever may change the index, the update must be forbidden
# - for count and sum: they are strictly connected, if avg is defined then sum is automatically created
#

def validate_collection_count(aggregation_configuration: AggregationConfiguration) -> bool:
    if aggregation_configuration.type == AggregationType.COLLECTION_COUNT:
        return any(
            elem in aggregation_configuration.on for elem in [AggregationTrigger.INSERT, AggregationTrigger.DELETE])

    return False


def validate_sum(aggregation_configuration: AggregationConfiguration) -> bool:
    result = True
    if aggregation_configuration.type == AggregationType.SUM:
        result = result and aggregation_configuration.target_field is not None
        result = result and aggregation_configuration.join is None
        result = result and aggregation_configuration.matches is None
    else:
        result = False

    return result


def validate(aggregation_configuration: AggregationConfiguration) -> bool:
    return {
        # AggregationType.AVG: aggregate,
        AggregationType.COLLECTION_COUNT: validate_collection_count,
        # AggregationType.AVG_JOIN: aggregate,
        # AggregationType.MAX: max,
        # AggregationType.MIN: min,
        # AggregationType.SUM: aggregate,
        AggregationType.SUM: validate_sum()
    }[aggregation_configuration.type](aggregation_configuration);


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


    def avg(aggregation_configuration: AggregationConfiguration,
            collection: Collection,
            new_record: dict,
            old_record: dict):
        ## load aggregation
        ## TODO: the index should optimize READ so no need to copy the value on the index row

        aggregations = map(lambda a: AggregationService.get_aggregation_by_name(a.name), AggregationService.get_aggregations_by_name_generator(aggregation_configuration.name))
        # count_target_field
        # sum_target_field
        # avg_target_field
        count_target_field_aggregation = next(filter(lambda a: isinstance(a, AggregationCount), aggregations),None)
        sum_target_field_aggregation = next(filter(lambda a: isinstance(a, AggregationSum), aggregations),None)
        avg_target_field_aggregation = next(filter(lambda a: isinstance(a, AggregationAvg), aggregations),None)

        record = (new_record or old_record)
        is_decrement = True if new_record is None else False
        _sum = 0
        _count = 1
        if aggregation_configuration.target_field in record:

            if count_target_field_aggregation:

                is_increment = True if old_record is None else False
                is_decrement = True if new_record is None else False

                increment = 1 if is_increment else -1
                AggregationService.increment(count_target_field_aggregation, "count", increment)
                _count = count_target_field_aggregation.count + increment
            else:
                try:
                    x = record[aggregation_configuration.target_field]
                    value = int(x)
                    if is_decrement:
                        value = value * -1
                    count_target_field_aggregation = AggregationService.create_aggregation(
                        AggregationCount("count_" + aggregation_configuration.name, aggregation_configuration.name, 1))
                    _count = 1
                except Exception as e:
                    logger.error("unable to count the value {} with message {}".format(x, e))
                    raise e

            if sum_target_field_aggregation:
                try:
                    x = record[aggregation_configuration.target_field]
                    value = int(x)
                    if is_decrement:
                        value = value * -1
                    sum_target_field_aggregation = AggregationService.increment(sum_target_field_aggregation, "sum", value)
                    logger.info("aggregation aftre update {} ".format(sum_target_field_aggregation))
                    _sum = sum_target_field_aggregation.sum


                except Exception as e:
                    logger.error("unable to count the value {} with message {}".format(x, e))
                    raise e
            else:
                try:
                    x = record[aggregation_configuration.target_field]
                    value = int(x)
                    if is_decrement:
                        value = value * -1
                    sum_target_field_aggregation = AggregationService.create_aggregation(
                        AggregationSum("sum_" + aggregation_configuration.name, aggregation_configuration.name, value))
                    _sum = value
                except Exception as e:
                    logger.error("unable to sum the value {} with message {}".format(x,e))
                    raise e

            logger.info("sum {} ".format(_sum))
            logger.info("count {} ".format(_count))
            avg = _sum / _count
            logger.info("avg is {} ".format(avg))
            if avg_target_field_aggregation:
                try:

                    avg_target_field_aggregation.avg = avg
                    return AggregationService.updateAggregation(avg_target_field_aggregation)
                except Exception as e:
                    logger.error("unable to sum the value {}".format(x))
                    raise e

            else:

                return AggregationService.create_aggregation(
                    AggregationAvg(aggregation_configuration.name, aggregation_configuration.name, avg))

    def sum(aggregation_configuration: AggregationConfiguration,
            collection: Collection,
            new_record: dict,
            old_record: dict):
        ## load aggregation
        aggregation = AggregationService.get_aggregation_by_name(aggregation_configuration.name)

        record = (new_record or old_record)
        is_decrement = True if new_record is None else False
        if aggregation:
            if isinstance(aggregation, AggregationSum):
                if aggregation_configuration.target_field in record:
                    try:
                        x = record[aggregation_configuration.target_field]
                        value = int(x)
                        if is_decrement:
                            value = value * -1
                        if AggregationService.increment(aggregation, "sum", value):
                            aggregation.sum = aggregation.sum + value
                    except:
                        logger.error("unable to sum the value {}".format(x))

            return aggregation
        else:
            ## create the aggregation if it doesn't exist
            if aggregation_configuration.target_field in record:
                try:
                    x = record[aggregation_configuration.target_field]
                    value = int(x)
                    if is_decrement:
                        value = value * -1
                    return AggregationService.create_aggregation(
                        AggregationSum(aggregation_configuration.name, aggregation_configuration.name, value))
                except:
                    logger.error("unable to sum the value {}".format(x))

    def collection_count(aggregation_configuration: AggregationConfiguration,
                         collection: Collection,
                         new_record: dict,
                         old_record: dict):
        ## load aggregation
        aggregation = AggregationService.get_aggregation_by_name(aggregation_configuration.name)

        ## TODO: validate also the operation type aggregation_configuration.on

        if aggregation:
            if isinstance(aggregation, AggregationCount):
                is_increment = True if old_record is None else False
                is_decrement = True if new_record is None else False
                if is_increment:
                    if AggregationService.increment_count(aggregation):
                        ## TODO: avoid set and copy
                        aggregation.count = aggregation.count + 1

                elif is_decrement:
                    if AggregationService.decrement_count(aggregation):
                        aggregation.count = aggregation.count - 1
            return aggregation

        else:
            ## create the aggregation if it doesn't exist
            logger.info("found aggregation configuration {}".format(aggregation_configuration))
            return AggregationService.create_aggregation(
                AggregationCount(aggregation_configuration.name, aggregation_configuration.name, 1))

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

    def max(aggregation: AggregationConfiguration, old_document: dict, new_document: dict):
        ## create "normal" index by field ordered by itself
        return None

    def min(aggregation: AggregationConfiguration, old_document: dict, new_document: dict):
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
        AggregationType.AVG: avg,
        AggregationType.COLLECTION_COUNT: collection_count,
        # AggregationType.AVG_JOIN: aggregate,
        # AggregationType.MAX: max,
        # AggregationType.MIN: min,
        AggregationType.SUM: sum,
        # AggregationType.SUM_COUNT: aggregate
    }

    @staticmethod
    def execute_aggregation(aggregation: AggregationConfiguration, collection: Collection, new_record: dict,
                            old_record: dict):
        if aggregation.matches:
            if not match_predicate(new_record, aggregation.matches):
                return
        return AggregationProcessingService.aggregation_executor_factory[aggregation.type](aggregation, collection, new_record,
                                                                                    old_record)
