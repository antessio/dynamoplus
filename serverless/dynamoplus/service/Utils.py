import logging
from datetime import datetime
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def getByKeyRecursive(data, keys, skipIfNotFound=True):
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
                logger.info("Key {} not found in {}".format(k,data))
                raise Exception("{} not found".format(k))
    return "#".join(result) if result else ""    
def findValue(d,keys):
    k = keys[0]
    if k in d:
        v=d[k]
        if isinstance(v, dict):
            return findValue(v,keys[1:])
        else:
            return convertToString(v)
    else:
        return None
def fromParametersToDict(queryParams):
    target = {}
    for k,v in queryParams.items():
        nextKey = k
        nextValue = v
        if "." in k:
            keySplitted = k.split(".")
            nextKey = keySplitted[0]
            if len(keySplitted) > 1:
                nextValue = _recursiveGet(".".join(keySplitted[1:]),v)
        if nextKey not in target:
            target[nextKey]=nextValue
        else:
            target[nextKey]= {**target[nextKey], **nextValue}
    return target

def _recursiveGet(key,value):
    if "." in key:
        keySplitted=key.split(".")
        if len(keySplitted)>1:
            nextKey=".".join(keySplitted[1:])
            result=(keySplitted[0], _recursiveGet(nextKey,value))
        else:
            result={keySplitted[0]: value}
    else:
        return {key: value}
    return result

    # def _recursiveGet(self,key,value):
    #     if "." in key:
    #         keySplitted=key.split(".")
    #         if len(keySplitted)>1:
    #             nextKey=".".join(keySplitted[1:])
    #             result=(keySplitted[0], self._recursiveGet(nextKey,value))
    #         else:
    #             result={keySplitted[0]: value}
    #     else:
    #         return {key: value}
    #     return result

def convertToString(val):
        if isinstance(val, datetime):
            logger.debug("converting datetime {} to string ".format(val))
            return str(decimal.Decimal(datetime.timestamp(val)))
        elif isinstance(val, decimal.Decimal):
            logger.debug("converting decimal {} to string ".format(val))
            return str(val)
        elif isinstance(val,bool):
            return "true" if val else "false"
        else:
            return val