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
    elif isinstance(val,bool):
        return "true" if val else "false"
    elif val in ['True', 'False']:
        return "true" if val=='True' else "false"
    else:
        return val

def findValue(d:dict,keys:List[str]):
    k = keys[0]
    if k in d:
        v=d[k]
        if isinstance(v, dict):
            return findValue(v,keys[1:])
        else:
            return convertToString(v)
    else:
        return None

def getValuesByKeyRecursive(data, keys, skipIfNotFound=True):
    result = []
    for k in keys:
        subKeys = k.split(".")
        if len(subKeys)>1:
            subVal = findValue(data,subKeys)
            if subVal is not None:
                result.append(findValue(data,subKeys))
        elif k in data:
            result.append(str(data[k]))
        else:
            if not skipIfNotFound:
                raise Exception("{} not found in {} ".format(k,data))
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