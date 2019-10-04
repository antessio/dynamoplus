import unittest
import collections
from collections.abc import Iterable, Mapping, ByteString, Set
import numbers
import decimal
from datetime import datetime,timezone
from dynamoplus.utils.utils import convertToString, findValue, getValuesByKeyRecursive

class TestUtils(unittest.TestCase):
    def test_convertDateTimeToString(self):
        self.assertEqual(convertToString(datetime(2019,12,12,8,8,10,100,tzinfo=timezone.utc)), "1576138090.0000998973846435546875")
    def test_convertDecimaToString(self):
        self.assertEqual(convertToString(decimal.Decimal('20.00')), "20.00")
    def test_convertBooleanToString(self):
        self.assertEqual(convertToString(True), "true")
    def test_convertBooleanStringToString(self):
        self.assertEqual(convertToString("True"), "true")

    def test_findValue(self):
        target = {"attribute_name_1": "value1","attribute_name_2":"value2"}
        value = findValue(target, ["attribute_name_1"])
        self.assertEqual(value,"value1")
    
    def test_findValueNested(self):
        target = {"attribute_name_1": { "attribute_name_11": {"attribute_name_111": "value1"}},"attribute_name_2":"value2"}
        value = findValue(target, ["attribute_name_1","attribute_name_11","attribute_name_111"])
        self.assertEqual(value,"value1")
    def test_findValueNotFound(self):
        target = {"attribute_name_1": "value1","attribute_name_2":"value2"}
        value = findValue(target, ["attribute_name_3"])
        self.assertIsNone(value)

    def test_getValueByKeyRecursive(self):
        target = {"attribute_name_1": { "attribute_name_11": {"attribute_name_111": "value1"}},"attribute_name_2":"value2"}
        value = getValuesByKeyRecursive(target, ["attribute_name_1.attribute_name_11.attribute_name_111","attribute_name_2"])
        self.assertTrue(len(value),2)
        self.assertEqual(value[0],"value1")
        self.assertEqual(value[1],"value2")