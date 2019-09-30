import json
from boto3.dynamodb.conditions import Key, Attr
import logging
from dynamoplus.service.Utils import getByKeyRecursive, findValue, convertToString
from dynamoplus.repository.Repository import IndexRepository,Repository

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class IndexService(object):
    def __init__(self, tableName, entityName,queryIndex, dynamoDB):
        indexUtils = IndexUtils()
        self.orderBy, self.index = indexUtils.buildIndex(queryIndex)
        indexKeys = self.index["conditions"]
        self.repository = IndexRepository(tableName,entityName,queryIndex,self.orderBy,indexKeys,dynamoDB=dynamoDB)

    def findByExample(self, entity, limit=None, startFrom=None):
        query={
            "entity": entity
        }
        if limit:
            query["limit"]=limit
        if self.orderBy is not None:
            query["orderBy"]=self.orderBy
        if startFrom:
            query["startFrom"] = startFrom
        return  self.repository.find(query)




class IndexUtils(object):
    
    def findIndexFromParameters(self, entityName, parameters):
        orderBy=None
        conditions=[]
        for k in parameters:
            if k.lower() == "orderby":
                orderBy=parameters[k].lower()
            else:
                conditions.append(k.lower())
        return entityName+"#"+"__".join(conditions)+("__ORDER_BY__"+orderBy if orderBy is not None else "")
    def _findIfNotNone(self,entity,c):
        v=findValue(entity, c.split("."))
        return {c: v} if v is not None else None
    def getValuesFromEntity(self, i, entity):
        orderBy, index = self.buildIndex(i)
        return dict( (c, findValue(entity, c.split("."))) for c in index["conditions"])
    def findIndexFromEntity(self,indexes,entity,entityName):
        matchingIndexes={}
        for i in indexes:
            orderBy, index = self.buildIndex(i)
            if index["tablePrefix"] == entityName:
                # my_dictionary = {k: f(v) for k, v in my_dictionary.items()}
                # my_dictionary = dict(map(lambda kv: (kv[0], f(kv[1])), my_dictionary.iteritems()))
                values = {}
                for c in index["conditions"]:
                    v = findValue(entity, c.split("."))
                    if v: 
                        values[c]=v
                # values = self._findIfNotNone(entity, c) for c in index["conditions"]
                if values:
                    foundIndex={ 
                        "tablePrefix": index["tablePrefix"],
                        "conditions": index["conditions"], 
                        "values": values
                    }
                    if orderBy: 
                        orderByValue = findValue(entity, orderBy.split("."))
                        if orderByValue:
                            foundIndex["orderBy"]=orderBy
                            foundIndex["orderByValue"]=orderByValue
                    matchingIndexes[i]=foundIndex
        return matchingIndexes

    def _findIndex(self, i, entity):
        orderBy, foundIndex = self.buildIndex(i)
        try:
            orderValue=getByKeyRecursive(entity,[orderBy]) if orderBy is not None else None
            if (orderValue is not None and orderBy is not None) or (orderValue is None and orderBy is None):
                foundIndex["orderBy"]=orderBy
                foundIndex["orderValue"]=orderValue
                return foundIndex
        except Exception as e:
            logger.warning("the order by key {} was not found".format(orderBy))
            return None

    def buildIndex(self, i):
        part1 = i.split("#")
        entityName=part1[0]
        queryPart = part1[1]
        part2 = queryPart.split("__ORDER_BY__")
        conditionsPart=part2[0]
        orderBy=part2[1] if len(part2)>1 else None
        logger.info("index {} and conditions {}".format(i,conditionsPart))
        conditionsList = conditionsPart.split("__")
        logger.info("Index for {}, attributes are {} ".format(entityName,conditionsList))
        foundIndex = {
            "tablePrefix": entityName,
            "conditions": conditionsList
        }
        return orderBy, foundIndex

    def dictDiffs(self,d1,d2):
        result={}
        for k in d1.keys():
            if k not in d2:
                logger.warning("key {} is not present in the target dict".format(k))
            else:
                if type(d1[k]) is dict:
                    result[k]=self.dictDiffs(d1[k],d2[k])
                else:
                    result[k]=d2[k]
        for k in d2.keys():
            if k not in d1:
                result[k]=d2[k]
        return result
