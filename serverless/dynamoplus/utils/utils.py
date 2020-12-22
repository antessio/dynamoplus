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


def convert_to_string(val):
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
            return convert_to_string(v)
    else:
        return None


def get_schema_from_conditions(conditions: List[str]):
    result = {}
    for k in conditions:
        sub_keys = k.split(".")
        if len(sub_keys) == 1:
            result[k] = {"type": "string"}
        else:
            k2 = ".".join(sub_keys[1:])
            r = __spread(k2)
            if sub_keys[0] not in result:
                result[sub_keys[0]] = {"type": "object", "properties": r}
            else:
                __dict_merge(result[sub_keys[0]]["properties"], r)
    return result


def __dict_merge(dct: dict, merge_dct: dict):
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


def filter_out_not_included_fields(d: dict, included_fields: List[str]):
    result = {}
    for f in included_fields:
        nested_fields = f.split('.')
        nested_value = None
        if len(nested_fields) > 1:
            if nested_fields[0] in d:
                nested_value = filter_out_not_included_fields(d[nested_fields[0]], nested_fields[1:])
        else:
            if nested_fields[0] in d:
                nested_value = d[nested_fields[0]]
        if nested_value:
            result[nested_fields[0]] = nested_value
    return result


def find_removed_values(before: dict, after: dict, include_changed_values: bool = False):
    removed_values = {}
    for k in before.keys():
        if after is None or k not in after:
            removed_values[k] = before[k]
        elif isinstance(before[k], dict) and (isinstance(after[k], dict)):
            v = find_removed_values(before[k], after[k])
            if v is not None:
                removed_values[k] = v

        elif isinstance(before[k], list) and isinstance(after[k], list):

            if len(before[k]) > len(after[k]):
                removed_values_list = []
                for before_value in before[k][len(after[k]):]:
                    removed_values_list.append(before_value)

                if len(removed_values_list) > 0:
                    removed_values[k] = removed_values_list
            elif len(before[k]) == len(after[k]):
                removed_values_list = []
                for i, after_value in enumerate(after[k]):
                    if isinstance(after_value, dict):
                        v = find_removed_values(before[k][i], after_value, True)
                        if v is not None:
                            removed_values_list.append(v)
                    elif after_value != before[k][i]:
                        removed_values_list.append(before[k][i])
                if len(removed_values_list) > 0:
                    removed_values[k] = removed_values_list
        elif before[k] != after[k] and include_changed_values:
            removed_values[k] = before[k]
    return removed_values if len(removed_values.keys()) > 0 else None


def find_added_values(before: dict, after: dict):
    added_values = {}
    for k in after.keys():
        if before is None or k not in before:
            added_values[k] = after[k]
        elif isinstance(before[k], dict) and (isinstance(after[k], dict)):
            v = find_added_values(before[k], after[k])
            if v is not None:
                added_values[k] = v

        elif isinstance(before[k], list) and isinstance(after[k], list):

            if len(before[k]) < len(after[k]):
                added_values_list = []
                for add_value in after[k][len(before[k]):]:
                    added_values_list.append(add_value)

                if len(added_values_list) > 0:
                    added_values[k] = added_values_list
            elif len(before[k]) == len(after[k]):
                added_values_list = []
                for i, after_value in enumerate(after[k]):
                    if isinstance(after_value, dict):
                        v = find_added_values(before[k][i], after_value)
                        if v is not None:
                            added_values_list.append(v)
                    elif after_value != before[k][i]:
                        added_values_list.append(after_value)
                if len(added_values_list) > 0:
                    added_values[k] = added_values_list

    return added_values if len(added_values.keys()) > 0 else None


def find_updated_values(before: dict, after: dict):
    updated_values = {}
    for k in after.keys():
        if k in before:
            if isinstance(before[k], dict) and (isinstance(after[k], dict)):
                v = find_updated_values(before[k], after[k])
                if v is not None:
                    updated_values[k] = v

            elif isinstance(before[k], list) and isinstance(after[k], list):
                new_values_list = []
                for i, before_value in enumerate(before[k]):
                    if before_value != after[k][i]:
                        new_values_list.append(after[k][i])
                if len(new_values_list) > 0:
                    updated_values[k] = new_values_list

            elif before[k] != after[k]:
                updated_values[k] = after[k]

    return updated_values if len(updated_values.keys()) > 0 else None
