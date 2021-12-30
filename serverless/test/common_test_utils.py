
import random
from enum import Enum

from dynamoplus.models.system.aggregation.aggregation import AggregationType, AggregationTrigger


def random_enum(enum: Enum):
    return random.choice(list(enum))

def random_aggregation_configuration_API_data():

    aggregation_type = random_enum(AggregationType).name
    return {
                "collection": {
                    "name": f'collection_name {random_value()}'
                },
                "type": aggregation_type,
                "configuration": {
                    "on": [
                        random_enum(AggregationTrigger).name
                    ],
                    "target_field": f'field_{random_value()}'
                },
                "name": f'name_{random_value()}',
                "aggregation": {
                    "name": f'aggregation_name_{random_value()}',
                    "type": aggregation_type,
                    "payload": {

                    }
                }
            }


def random_value():
    return random.randrange(1, 30)



