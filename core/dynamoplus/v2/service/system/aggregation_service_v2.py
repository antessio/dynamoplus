import abc
import logging
import uuid
from decimal import Decimal

from dynamoplus.models.query.conditions import match_predicate
from dynamoplus.models.system.aggregation.aggregation import AggregationType, AggregationTrigger
from dynamoplus.v2.service.system.system_service_v2 import AggregationConfiguration, Collection, AggregationCount, \
    AggregationSum, AggregationAvg, AggregationService, AggregationConfigurationService, AggregationCountCreateCommand, \
    AggregationSumCreateCommand, AggregationAvgCreateCommand

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


class AggregationExecutor(abc.ABC):
    aggregation_service: AggregationService
    aggregation_configuration_service: AggregationConfigurationService

    def __init__(self, aggregation_configuration_service: AggregationConfigurationService,
                 aggregation_service: AggregationService):
        self.aggregation_configuration_service = aggregation_configuration_service
        self.aggregation_service = aggregation_service

    @abc.abstractmethod
    def execute_aggregation(self, aggregation_configuration: AggregationConfiguration, new_record: dict,
                            old_record: dict):
        raise NotImplementedError()


class AvgAggregationExecutor(AggregationExecutor):

    def execute_aggregation(self, aggregation_configuration: AggregationConfiguration, new_record: dict,
                            old_record: dict):
        ## load aggregation
        ## TODO: the index should optimize READ so no need to copy the value on the index row

        aggregations = list(
            self.aggregation_service.get_aggregations_by_configuration_name_generator(aggregation_configuration.name))

        # count_target_field
        # sum_target_field
        # avg_target_field
        count_target_field_aggregation = next(filter(lambda a: isinstance(a, AggregationCount), aggregations), None)
        sum_target_field_aggregation = next(filter(lambda a: isinstance(a, AggregationSum), aggregations), None)
        avg_target_field_aggregation = next(filter(lambda a: isinstance(a, AggregationAvg), aggregations), None)

        record = (new_record or old_record)
        is_decrement = True if new_record is None else False
        _sum = 0
        _count = 1
        if aggregation_configuration.target_field in record:

            if count_target_field_aggregation:

                is_increment = True if old_record is None else False
                is_decrement = True if new_record is None else False

                increment = 1 if is_increment else -1
                result = self.aggregation_service.increment(count_target_field_aggregation, Decimal(increment))
                _count = result.count
            else:
                try:
                    x = record[aggregation_configuration.target_field]
                    count_target_field_aggregation = self.aggregation_service.create_aggregation(
                        AggregationCountCreateCommand("count_" + aggregation_configuration.name,
                                         aggregation_configuration.name, 1))
                    _count = 1
                except Exception as e:
                    logger.error("unable to count the value", e)
                    raise e

            if sum_target_field_aggregation:
                try:
                    x = record[aggregation_configuration.target_field]
                    value = int(x)
                    if is_decrement:
                        value = value * -1
                    sum_target_field_aggregation = self.aggregation_service.increment(sum_target_field_aggregation,
                                                                                      Decimal(value))
                    logger.info("aggregation after update {} ".format(sum_target_field_aggregation))
                    _sum = sum_target_field_aggregation.sum


                except Exception as e:
                    logger.error("unable to count the value ", e )
                    raise e
            else:
                try:
                    x = record[aggregation_configuration.target_field]
                    value = int(x)
                    if is_decrement:
                        value = value * -1
                    sum_target_field_aggregation = self.aggregation_service.create_aggregation(
                        AggregationSumCreateCommand("sum_" + aggregation_configuration.name,
                                       aggregation_configuration.name, value))
                    _sum = value
                except Exception as e:
                    logger.error("unable to sum the value with message", e)
                    raise e

            logger.info("sum {} ".format(_sum))
            logger.info("count {} ".format(_count))
            avg = _sum / _count
            logger.info("avg is {} ".format(avg))
            if avg_target_field_aggregation:
                try:

                    return self.aggregation_service.update_aggregation(AggregationAvg(avg_target_field_aggregation.id, avg_target_field_aggregation.name, avg_target_field_aggregation.configuration_name, avg))
                except Exception as e:
                    logger.error("unable to sum the value ", e)
                    raise e

            else:

                return self.aggregation_service.create_aggregation(
                    AggregationAvgCreateCommand("avg_" + aggregation_configuration.name, aggregation_configuration.name, avg))


class SumAggregationExecutor(AggregationExecutor):

    def execute_aggregation(self, aggregation_configuration: AggregationConfiguration, new_record: dict,
                            old_record: dict):
        ## load aggregation
        sum_target_field_aggregation = next(filter(lambda a: isinstance(a, AggregationSum),
                                                   self.aggregation_service.get_aggregations_by_configuration_name_generator(
                                                       aggregation_configuration.name)), None)

        record = (new_record or old_record)
        is_decrement = True if new_record is None else False
        if sum_target_field_aggregation:
            if isinstance(sum_target_field_aggregation, AggregationSum):
                if aggregation_configuration.target_field in record:
                    try:
                        x = record[aggregation_configuration.target_field]
                        value = int(x)
                        if is_decrement:
                            value = value * -1
                        result = self.aggregation_service.increment(sum_target_field_aggregation, Decimal(value))
                        sum_target_field_aggregation.sum = result.sum
                    except Exception as e:
                        logger.error("unable to sum the value", e)

            return sum_target_field_aggregation
        else:
            ## create the aggregation if it doesn't exist
            if aggregation_configuration.target_field in record:
                try:
                    x = record[aggregation_configuration.target_field]
                    value = int(x)
                    if is_decrement:
                        value = value * -1
                    return self.aggregation_service.create_aggregation(
                        AggregationSumCreateCommand( aggregation_configuration.name, aggregation_configuration.name, value))
                except Exception as e:
                    logger.error("unable to sum the value ", e)


class CountAggregationExecutor(AggregationExecutor):
    def execute_aggregation(self, aggregation_configuration: AggregationConfiguration, new_record: dict,
                            old_record: dict):
        ## load aggregation
        count_target_field_aggregation = next(filter(lambda a: isinstance(a, AggregationCount),
                                                     self.aggregation_service.get_aggregations_by_configuration_name_generator(
                                                         aggregation_configuration.name)
                                                     ), None)
        if count_target_field_aggregation:
            logger.info("aggregation already exists {} ".format(count_target_field_aggregation))
            if isinstance(count_target_field_aggregation, AggregationCount):
                is_increment = True if new_record and old_record is None else False
                is_decrement = True if new_record is None else False
                if is_increment:
                    if self.aggregation_service.increment_count(count_target_field_aggregation):
                        logger.info("increment count count {} ".format(aggregation_configuration.name))
                        count_target_field_aggregation = AggregationCount(count_target_field_aggregation.id,
                                                                          count_target_field_aggregation.name,
                                                                          count_target_field_aggregation.configuration_name,
                                                                          count_target_field_aggregation.count + 1)

                elif is_decrement:
                    if self.aggregation_service.decrement_count(count_target_field_aggregation):
                        logger.info("decrement count count {} ".format(aggregation_configuration.name))
                        count_target_field_aggregation = AggregationCount(count_target_field_aggregation.id,
                                                                          count_target_field_aggregation.name,
                                                                          count_target_field_aggregation.configuration_name,
                                                                          count_target_field_aggregation.count - 1)
            return count_target_field_aggregation

        else:
            ## create the aggregation if it doesn't exist
            logger.info("creating new aggregation count {} ".format(aggregation_configuration.name))
            return self.aggregation_service.create_aggregation(
                AggregationCountCreateCommand("count_" + aggregation_configuration.name,
                                 aggregation_configuration.name,
                                 1))


class AggregationProcessingService:
    aggregation_service: AggregationService
    aggregation_configuration: AggregationConfiguration
    aggregation_configuration_service: AggregationConfigurationService

    def __init__(self, aggregation_configuration: AggregationConfiguration,
                 aggregation_service: AggregationService,
                 aggregation_configuration_service: AggregationConfigurationService):
        # self.aggregation_service = AggregationService()
        # self.aggregation_configuration_service = AggregationConfigurationService()
        self.aggregation_service = aggregation_service
        self.aggregation_configuration_service = aggregation_configuration_service
        self.aggregation_configuration = aggregation_configuration
        self.aggregation_executor_factory = {
            AggregationType.AVG: AvgAggregationExecutor(self.aggregation_configuration_service,
                                                        self.aggregation_service),
            AggregationType.COLLECTION_COUNT: CountAggregationExecutor(self.aggregation_configuration_service,
                                                                       self.aggregation_service),
            AggregationType.SUM: SumAggregationExecutor(self.aggregation_configuration_service,
                                                        self.aggregation_service),
        }

    def avg_join(self,
                 collection: Collection, new_record: dict,
                 old_record: dict):
        ## Load Collection by `aggregation_configuration.join.collection_name`
        ## Load document by id using `aggregation_configuration.join.using_field` in document
        ## Use the loaded document to calculate sum, count and avg

        return None

    def max(self, old_document: dict, new_document: dict):
        ## create "normal" index by field ordered by itself
        return None

    def min(self, old_document: dict, new_document: dict):
        ## create "normal" index by field order by itself
        return None

    def execute_aggregation(self,
                            collection: Collection,
                            new_record: dict,
                            old_record: dict):
        if self.aggregation_configuration.matches:
            if not match_predicate(new_record, self.aggregation_configuration.matches):
                logger.debug(
                    "aggregation {} not matching  the predicate ".format(self.aggregation_configuration.__str__()))
                return
        if old_record is None:
            aggregation_trigger = AggregationTrigger.INSERT
        else:
            aggregation_trigger = AggregationTrigger.DELETE if new_record is None else AggregationTrigger.UPDATE

        if self.aggregation_configuration.on is None or aggregation_trigger in self.aggregation_configuration.on:
            executor = self.aggregation_executor_factory[self.aggregation_configuration.type]
            logger.info("executing aggregation on {} - {}".format(collection.name, self.aggregation_configuration.name))
            return executor.execute_aggregation(self.aggregation_configuration,
                                                new_record,
                                                old_record)
        else:
            logger.debug(
                "aggregation {} not matching trigger {} ".format(self.aggregation_configuration, aggregation_trigger))
