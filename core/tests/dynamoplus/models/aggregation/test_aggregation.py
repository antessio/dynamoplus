import unittest

from dynamoplus.models.query.conditions import Eq, And
from dynamoplus.models.system.aggregation.aggregation import AggregationConfiguration, AggregationType, AggregationTrigger


class TestAggregation(unittest.TestCase):
    def test_aggregation_name_simple(self):
        a = AggregationConfiguration("restaurant", AggregationType.COLLECTION_COUNT, [AggregationTrigger.INSERT], None, None, None)
        self.assertEqual("restaurant_collection_count", a.name)

    def test_aggregation_name_target_field(self):
        a = AggregationConfiguration("review", AggregationType.AVG, [AggregationTrigger.INSERT], "rate", None, None)
        self.assertEqual("review_avg_rate", a.name)

    def test_aggregation_name_conditions(self):
        a = AggregationConfiguration("restaurant", AggregationType.AVG, [AggregationTrigger.INSERT], "seat", Eq("type", "pizzeria"),
                                     None)
        self.assertEqual("restaurant_type_pizzeria_avg_seat", a.name)

    def test_aggregation_name_conditions_and(self):
        a = AggregationConfiguration("restaurant", AggregationType.AVG, [AggregationTrigger.INSERT], "seat",
                                     And([Eq("active", "true"), Eq("type", "pizzeria")]), None)
        self.assertEqual("restaurant_active_type_true_pizzeria_avg_seat", a.name)

    @unittest.skip("Still implementing")
    def test_list_types(self):
        self.assertEqual(["COLLECTION_COUNT", "AVG", "AVG_JOIN", "SUM", "SUM_COUNT", "MIN", "MAX"],
                         AggregationType.types())
