import logging

from dynamoplus.models.query.conditions import match_predicate
from dynamoplus.models.system.aggregation.aggregation import AggregationConfiguration, AggregationType, AggregationCount, AggregationTrigger, AggregationSum, AggregationAvg
from dynamoplus.models.system.collection.collection import Collection

from dynamoplus.v2.service.system.system_service import AggregationService

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# TODO
#
# - for MAX and MIN: an index by the field must be present so if it doesn't exist, then create it and make it unmodifiable
#   - in the API or whatever may change the index, the update must be forbidden
# - for count and sum: they are strictly connected, if avg is defined then sum is automatically created
#

# def validate_collection_count(aggregation_configuration: AggregationConfiguration) -> bool:
#     if aggregation_configuration.type == AggregationType.COLLECTION_COUNT:
#         return any(
#             elem in aggregation_configuration.on for elem in [AggregationTrigger.INSERT, AggregationTrigger.DELETE])
#
#     return False
#
#
# def validate_sum(aggregation_configuration: AggregationConfiguration) -> bool:
#     result = True
#     if aggregation_configuration.type == AggregationType.SUM:
#         result = result and aggregation_configuration.target_field is not None
#         result = result and aggregation_configuration.join is None
#         result = result and aggregation_configuration.matches is None
#     else:
#         result = False
#
#     return result
#
#
# def validate(aggregation_configuration: AggregationConfiguration) -> bool:
#     return {
#         # AggregationType.AVG: aggregate,
#         AggregationType.COLLECTION_COUNT: validate_collection_count,
#         # AggregationType.AVG_JOIN: aggregate,
#         # AggregationType.MAX: max,
#         # AggregationType.MIN: min,
#         # AggregationType.SUM: aggregate,
#         AggregationType.SUM: validate_sum()
#     }[aggregation_configuration.type](aggregation_configuration);


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

    def avg_join(aggregation_configuration: AggregationConfiguration, collection: Collection, new_record: dict, old_record: dict):
        ## Load Collection by `aggregation_configuration.join.collection_name`
        ## Load document by id using `aggregation_configuration.join.using_field` in document
        ## Use the loaded document to calculate sum, count and avg

        return None

    def max(aggregation: AggregationConfiguration, old_document: dict, new_document: dict):
        ## create "normal" index by field ordered by itself
        return None

    def min(aggregation: AggregationConfiguration, old_document: dict, new_document: dict):
        ## create "normal" index by field order by itself
        return None



    aggregation_executor_factory = {
        AggregationType.AVG: avg,
        AggregationType.COLLECTION_COUNT: collection_count,
        AggregationType.SUM: sum,
    }

    @staticmethod
    def execute_aggregation(aggregation_configuration: AggregationConfiguration, collection: Collection,
                            new_record: dict,
                            old_record: dict):
        if aggregation_configuration.matches:
            if not match_predicate(new_record, aggregation_configuration.matches):
                logger.debug("aggregation {} not matching  the predicate ".format(aggregation_configuration.__str__()))
                return
        aggregation_trigger = AggregationTrigger.INSERT if old_record is None else AggregationTrigger.DELETE if new_record is None else AggregationTrigger.UPDATE
        if aggregation_configuration.on is None or aggregation_trigger in aggregation_configuration.on:
            return AggregationProcessingService.aggregation_executor_factory[aggregation_configuration.type](aggregation_configuration, collection, new_record,
                                                                                                         old_record)
        else:
            logger.debug("aggregation {} not matching trigger {} ".format(aggregation_configuration,aggregation_trigger))
