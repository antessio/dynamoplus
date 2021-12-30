from decimal import Decimal
from unittest import TestCase

from dynamoplus.utils.decimalencoder import DecimalEncoder


class TestDecimalEncoder(TestCase):
    def test_default_decimal(self):
        expected = 1020.00
        result = DecimalEncoder().default(Decimal(expected))
        self.assertEqual(result, expected)


