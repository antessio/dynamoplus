import collections
from collections.abc import Iterable, Mapping, ByteString, Set
import numbers
import decimal
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import boto3
from dynamoplus.service.Utils import getByKeyRecursive, findValue
import logging
import json
logging.basicConfig(level=logging.DEBUG)

def _convertToString(val):
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
def _sanitize(data):
    """ Sanitizes an object so it can be updated to dynamodb (recursive) """
    if not data and isinstance(data, (str, Set)):
        new_data = ""  # empty strings/sets are forbidden by dynamodb
    elif isinstance(data, (str, bool)):
        new_data = data  # important to handle these one before sequence and int!
    elif isinstance(data, Mapping):
        new_data = {key: _sanitize(data[key]) for key in data}
    elif isinstance(data, collections.abc.Sequence):
        new_data = [_sanitize(item) for item in data]
    elif isinstance(data, Set):
        new_data = {_sanitize(item) for item in data}
    elif isinstance(data, (float, int, complex)):
        new_data = context.create_decimal(data)
    elif isinstance(data, datetime):
        new_data = data.isoformat()
    else:
        new_data = data
    return new_data

class Repository(object):
    def __init__(self,tableName, entityPrefix,primaryKey,orderKey,dynamoDB=None):
        logging.basicConfig(level=logging.INFO)
        self.dynamoDB = dynamoDB if dynamoDB else boto3.resource('dynamodb')
        self.table = self.dynamoDB.Table(tableName)
        self.primaryKey = primaryKey
        self.orderKey = orderKey
        self.entityPrefix = entityPrefix
    
    def getPrimaryKey(self,entity):
        return self.entityPrefix+"#"+entity[self.primaryKey]
    def getSecondaryKey(self):        
        return self.entityPrefix
    
    def getData(self,entity, query=False):
        logging.info("orderKey {}".format(self.orderKey))
        orderingPart = _convertToString(findValue(entity,self.orderKey.split("."))) if self.orderKey is not None and query is False else None
        logging.debug("orderingPart {}".format(orderingPart))
        if orderingPart is not None:
            return orderingPart
        else:
            return _convertToString(entity[self.primaryKey])
            
    
    def create(self, entity):
        entity["pk"] = self.getPrimaryKey(entity)
        entity["sk"] = self.getSecondaryKey()
        entity["data"] = self.getData(entity)
        response = self.table.put_item(Item=_sanitize(entity))
        logging.info("Response from put item operation is "+response.__str__())
        return entity
    def get(self, id):
        # TODO: copy from query -> if the indexKeys is empty then get by primary key, otherwise get by global secondary index
        # it means if needed first get from index, then by primary key or, in case of index it throws a non supported operation exception
        result = self.table.get_item(
        Key={
            'pk': self.entityPrefix+"#"+id,
            'sk': self.entityPrefix
        })

        return result[u'Item'] if 'Item' in result else None
    def update(self, entity):
        entityDynamo = _sanitize(entity)
        if entityDynamo.keys:
            updateExpression = "SET "+", ".join(map(lambda k: k+"= :"+k, filter(lambda k: k != self.primaryKey and k!="pk" and k!="sk" and k!="data", entityDynamo.keys())))
            #expressionValues = map(lambda k,v: ":"+k filter(lambda k,v: k != self.primaryKey, entity.items())
            expressionValue = dict(
                map(lambda kv: (":"+kv[0],kv[1]), 
                filter(lambda kv: kv[0] != self.primaryKey and kv[0]!="pk" and kv[0] !="sk" and kv[0] !="data", entityDynamo.items())))
            response = self.table.update_item(
                Key={
                    'pk': self.getPrimaryKey(entityDynamo),
                    'sk': self.getSecondaryKey()
                },
                UpdateExpression=updateExpression,
                ExpressionAttributeValues=expressionValue,
                ReturnValues="UPDATED_NEW"
            )
            logging.info("Response from update operation is "+response.__str__())
            return entity
        else:
            return entity
    def delete(self, id):
        self.table.delete_item(
            Key={
                'pk': self.entityPrefix+"#"+id,
                'sk': self.getSecondaryKey()
            }
            )
    def find(self, query):
        entity = query["entity"]
        orderBy = query["orderBy"] if "orderBy" in query else None
        limit = query["limit"] if "limit" in query else None
        startFrom = query["startFrom"] if "startFrom" in query else None
        ## if order by begins with 
        if orderBy is None:
            key=Key('sk').eq(self.getSecondaryKey()) & Key('data').eq(self.getData(entity))
            logging.info("The key that will be used is sk={} data={}".format(self.getSecondaryKey(), self.getData(entity,True)))
        else:
            key=Key('sk').eq(self.getSecondaryKey()) & Key('data').begins_with(self.getData(entity,True))
            logging.info("The key that will be used is sk={} begings_with data={}".format(self.getSecondaryKey(), self.getData(entity,True)))
            
        
        dynamoQuery=dict(
            IndexName="sk-data-index",
            KeyConditionExpression=key,
            Limit=limit,
            ExclusiveStartKey=startFrom
        )
        response = self.table.query(
                **{k: v for k, v in dynamoQuery.items() if v is not None}
            )
        logging.info("Response from dynamo db {}".format(str(response)))
        lastKey=None
        if 'LastEvaluatedKey' in response:
            lastKey=json.dumps(response['LastEvaluatedKey'], separators=(',',':'))
        return {
            "data": list(map(lambda i: self.getEntityDTO(i), response[u'Items'])),
            "lastKey": lastKey
        }
    def getEntityDTO(self, entity):
        return {k: v for k, v in entity.items() if k not in ["pk","sk","data"]}
    


context = decimal.Context(
    Emin=-128, Emax=126, rounding=None, prec=38,
    traps=[decimal.Clamped, decimal.Overflow, decimal.Underflow]
)

class IndexRepository(Repository):
    def __init__(self,tableName, entityPrefix,primaryKey,orderKey,indexKeys, dynamoDB=None ):
        # ## first element of the indexKeys is the entityPrefix 
        super(IndexRepository, self).__init__(tableName, entityPrefix, primaryKey,orderKey,dynamoDB)
        self.indexKeys = indexKeys
    def getData(self,entity, query=False):
        logging.info("orderKey {}".format(self.orderKey))
        orderingPart = _convertToString(findValue(entity,self.orderKey.split("."))) if self.orderKey is not None and query is False else None
        logging.debug("orderingPart {}".format(orderingPart))
        logging.info("Entity {}".format(str(entity)))
        if self.indexKeys:
            logging.info("Index keys {}".format(self.indexKeys))
            keyPart = _convertToString(getByKeyRecursive(entity,self.indexKeys,not query))
            return keyPart+("#"+orderingPart if orderingPart is not None else "")
        elif orderingPart is not None:
            return orderingPart
        else:
            return _convertToString(entity[self.primaryKey])
    def getSecondaryKey(self):        
        return self.entityPrefix+"#"+"#".join(map(lambda x:x,self.indexKeys)) if self.indexKeys else self.entityPrefix