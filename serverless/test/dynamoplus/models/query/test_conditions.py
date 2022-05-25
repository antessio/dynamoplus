import unittest

from dynamoplus.models.query.conditions import And, Range, Eq, get_range_predicate, is_valid, \
    get_field_names_in_order, AnyMatch, match_predicate


class TestConditions(unittest.TestCase):

    def test_any_match(self):
        predicate = AnyMatch()
        self.assertFalse(predicate.is_range())
        self.assertEqual([], predicate.get_values())
        self.assertEqual([], predicate.get_fields())

    def test_match_predicate_eq(self):
        eq_condition = Eq("field1", "value1")
        self.assertTrue(match_predicate({"field1": "value1", "any_field": "value1"}, eq_condition))

    def test_not_match_predicate_eq(self):
        eq_condition = Eq("field1", "valueX")
        self.assertFalse(match_predicate({"field1": "value1", "any_field": "value1"}, eq_condition))

    def test_not_match_predicate_eq2(self):
        eq_condition = Eq("field1", "value1")
        self.assertFalse(match_predicate({"any_field": "value1"}, eq_condition))

    def test_match_predicate_range(self):
        range_condition = Range("field1", "value0", "value2")
        self.assertTrue(match_predicate({"field1": "value1", "any_field": "value1"}, range_condition))

    def test_not_match_predicate_range(self):
        range_condition = Range("field1", "value10", "value12")
        self.assertFalse(match_predicate({"field1": "value1", "any_field": "value1"}, range_condition))

    def test_not_match_predicate_range2(self):
        range_condition = Range("field1", "value10", "value12")
        self.assertFalse(match_predicate({"any_field": "value1"}, range_condition))

    def test_match_predicate_and(self):
        and_condition = And([Eq("field2", "value1"), Range("field1", "value0", "value2")])
        self.assertTrue(match_predicate({"field2": "value1", "field1": "value1", "any_field": "value1"}, and_condition))

    def test_not_match_predicate_and(self):
        and_condition = And([Eq("field1", "value2"), Range("field2", "X", "Y")])
        self.assertFalse(
            match_predicate({"field1": "value1", "field2": "value11", "any_field": "value1"}, and_condition))

        self.assertFalse(
            match_predicate({"field1": "value2", "field2": "value1111", "any_field": "value1"}, and_condition))

    def test_eq_to_data(self):
        eq_condition = Eq("field1", "value1")
        self.assertEqual("eq(field1)", eq_condition.to_string())

    def test_range_to_data(self):
        range_condition = Range("field1", "value1", "value2")
        self.assertEqual("range(field1)", range_condition.to_string())

    def test_and_to_data_simple(self):
        and_condition = And([Eq("field1", "value1"), Eq("field2", "value2")])
        self.assertEqual("and(eq(field1)__eq(field2))", and_condition.to_string())

    def test_and_to_data_range(self):
        and_condition = And([Eq("field1", "value1"), Range("field2", "value2", "value3")])
        self.assertEqual("and(eq(field1)__range(field2))", and_condition.to_string())

    def test_and_to_data_nested(self):
        nested = And([Eq("fieldA", "valueA"), Eq("fieldB", "valueB"), Eq("fieldC", "valueC")])
        and_condition = And([Eq("field1", "value1"), Range("field2", "value2", "value3"), nested])
        self.assertEqual("and(eq(field1)__range(field2)__and(eq(fieldA)__eq(fieldB)__eq(fieldC)))",
                         and_condition.to_string())

    def test_is_valid(self):
        nested = And([Eq("fieldA", "valueA"), Eq("fieldB", "valueB"), Eq("fieldC", "valueC")])
        self.assertTrue(is_valid(nested))
        nested = And([Eq("fieldA", "valueA"), Eq("fieldB", "valueB"), Range("fieldC", "valueC", "valueD")])
        self.assertTrue(is_valid(nested))
        nested = And([Eq("fieldA", "valueA"), Range("fieldB", "valueB", "value1"), Range("fieldC", "valueC", "valueD")])
        self.assertFalse(is_valid(nested))

    def test_get_range(self):
        nested = And([Eq("fieldA", "valueA"), Eq("fieldB", "valueB"), Eq("fieldC", "valueC")])
        self.assertEqual(None, get_range_predicate(nested))
        expected_range = Range("fieldC", "valueC", "valueD")
        nested = And([Eq("fieldA", "valueA"), Eq("fieldB", "valueB"), expected_range])
        self.assertEqual(expected_range, get_range_predicate(nested))
        expected_range = Range("fieldB", "valueB", "value1")
        nested = And([Eq("fieldA", "valueA"), expected_range, Range("fieldC", "valueC", "valueD")])
        self.assertEqual(expected_range, get_range_predicate(nested))

    def test_get_conditions_in_order(self):
        nested = And([Eq("fieldA", "valueA"), Eq("fieldB", "valueB"), Eq("fieldC", "valueC")])
        field_names = get_field_names_in_order(nested)
        self.assertEqual(["fieldA", "fieldB", "fieldC"], field_names)
        nested = And(
            [Eq("fieldA", "valueA"), Eq("fieldB", "valueB"), And([Eq("fieldC", "valueC"), Eq("fieldD", "valueD")])])
        field_names = get_field_names_in_order(nested)
        self.assertEqual(["fieldA", "fieldB", "fieldC", "fieldD"], field_names)
        nested = And(
            [Eq("fieldA", "valueA"), Eq("fieldB", "valueB"), Range("fieldC", "valueD1", "valueD2")])
        field_names = get_field_names_in_order(nested)
        self.assertEqual(["fieldA", "fieldB", "fieldC"], field_names)
