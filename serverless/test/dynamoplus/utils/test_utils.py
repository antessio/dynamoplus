import decimal
import unittest
from datetime import datetime, timezone

from dynamoplus.utils.utils import convertToString, find_value, get_values_by_key_recursive, get_schema_from_conditions


class TestUtils(unittest.TestCase):

    def test_get_schema_from_conditions_plain(self):
        result = get_schema_from_conditions(["attribute1", "attribute2"])
        self.assertDictEqual({"attribute1": {"type": "string"}, "attribute2": {"type": "string"}}, result)

    def test_get_schema_from_conditions_multi_level(self):
        result = get_schema_from_conditions(["attribute1.attribute11.attribute111",
                                            "attribute1.attribute11.attribute112",
                                            "attribute1.attribute12",
                                            "attribute2"])
        expected = {"attribute1":
                        {"type": "object",
                         "properties": {
                             "attribute11": {
                                 "type": "object",
                                 "properties": {
                                     "attribute111": {"type": "string"},
                                     "attribute112": {"type": "string"}
                                 }
                             },
                             "attribute12": {"type": "string"}
                         }
                         },
                    "attribute2": {"type": "string"}
                    }
        self.assertDictEqual(expected, result)

    def test_convertDateTimeToString(self):
        self.assertEqual(convertToString(datetime(2019, 12, 12, 8, 8, 10, 100, tzinfo=timezone.utc)),
                         "1576138090.0000998973846435546875")

    def test_convertDecimaToString(self):
        self.assertEqual(convertToString(decimal.Decimal('20.00')), "20.00")

    def test_convertBooleanToString(self):
        self.assertEqual(convertToString(True), "true")

    def test_convertBooleanStringToString(self):
        self.assertEqual(convertToString("True"), "true")

    def test_findValue(self):
        target = {"attribute_name_1": "value1", "attribute_name_2": "value2"}
        value = find_value(target, ["attribute_name_1"])
        self.assertEqual(value, "value1")

    def test_findValueNested(self):
        target = {"attribute_name_1": {"attribute_name_11": {"attribute_name_111": "value1"}},
                  "attribute_name_2": "value2"}
        value = find_value(target, ["attribute_name_1", "attribute_name_11", "attribute_name_111"])
        self.assertEqual(value, "value1")

    def test_findValueNotFound(self):
        target = {"attribute_name_1": "value1", "attribute_name_2": "value2"}
        value = find_value(target, ["attribute_name_3"])
        self.assertIsNone(value)

    def test_getValueByKeyRecursive(self):
        target = {"attribute_name_1": {"attribute_name_11": {"attribute_name_111": "value1"}},
                  "attribute_name_2": "value2"}
        value = get_values_by_key_recursive(target,
                                            ["attribute_name_1.attribute_name_11.attribute_name_111",
                                             "attribute_name_2"])
        self.assertTrue(len(value), 2)
        self.assertEqual(value[0], "value1")
        self.assertEqual(value[1], "value2")
