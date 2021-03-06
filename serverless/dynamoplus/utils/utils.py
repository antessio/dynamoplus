import collections
from collections.abc import Iterable, Mapping, ByteString, Set
import numbers
import decimal
from datetime import datetime
from typing import *
import logging

logging.basicConfig(level=logging.DEBUG)


def auto_str(cls):
    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )

    cls.__str__ = __str__
    return cls


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
        elif isinstance(v, List):
            return v
        else:
            return convertToString(v)
    else:
        return None


def get_schema_from_conditions(conditions: List[str]):
    result = {}
    for k in conditions:
        sub_keys = k.split(".")
        if len(sub_keys) == 1:
            result[k] = {"type":"string"}
        else:
            k2 = ".".join(sub_keys[1:])
            r = __spread(k2)
            if sub_keys[0] not in result:
                result[sub_keys[0]] = {"type": "object", "properties": r}
            else:
                __dict_merge(result[sub_keys[0]]["properties"], r)
    return result


def __dict_merge(dct:dict, merge_dct:dict):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], collections.Mapping)):
            __dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]


def __spread(s: str):
    sub_keys = s.split(".")
    if len(sub_keys) == 1:
        return {sub_keys[0]: {"type": "string"}}
    else:
        k = ".".join(sub_keys[1:])
        r = {sub_keys[0]: {"type": "object", "properties": __spread(k)}}
        return r


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
