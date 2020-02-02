import collections
from collections.abc import Iterable, Mapping, ByteString, Set
import numbers
import decimal
from datetime import datetime
from typing import *
import logging

logging.basicConfig(level=logging.DEBUG)


def convertToString(val):
    if isinstance(val, datetime):
        logging.debug("converting datetime {} to string ".format(val))
        return str(decimal.Decimal(datetime.timestamp(val)))
    elif isinstance(val, decimal.Decimal):
        logging.debug("converting decimal {} to string ".format(val))
        return str(val)
    elif isinstance(val, bool):
        return "true" if val else "false"
    elif val in ['True', 'False']:
        return "true" if val == 'True' else "false"
    else:
        return val


def find_value(d: dict, keys: List[str]):
    k = keys[0]
    if k in d:
        v = d[k]
        if isinstance(v, dict):
            return find_value(v, keys[1:])
        else:
            return convertToString(v)
    else:
        return None


def get_values_by_key_recursive(data, keys, skip_if_not_found=True):
    result = []
    for k in keys:
        sub_keys = k.split(".")
        if len(sub_keys) > 1:
            sub_val = find_value(data, sub_keys)
            if sub_val is not None:
                result.append(find_value(data, sub_keys))
        elif k in data:
            result.append(str(data[k]))
        else:
            if not skip_if_not_found:
                raise Exception("{} not found in {} ".format(k, data))
    return result


context = decimal.Context(
    Emin=-128, Emax=126, rounding=None, prec=38,
    traps=[decimal.Clamped, decimal.Overflow, decimal.Underflow]
)


def sanitize(data):
    """ Sanitizes an object so it can be updated to dynamodb (recursive) """
    if not data and isinstance(data, (str, Set)):
        new_data = ""  # empty strings/sets are forbidden by dynamodb
    elif isinstance(data, (str, bool)):
        new_data = data  # important to handle these one before sequence and int!
    elif isinstance(data, Mapping):
        new_data = {key: sanitize(data[key]) for key in data}
    elif isinstance(data, collections.abc.Sequence):
        new_data = [sanitize(item) for item in data]
    elif isinstance(data, Set):
        new_data = {sanitize(item) for item in data}
    elif isinstance(data, (float, int, complex)):
        new_data = context.create_decimal(data)
    elif isinstance(data, datetime):
        new_data = data.isoformat()
    else:
        new_data = data
    return new_data
