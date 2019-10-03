import unittest
import decimal
from dynamoplus.service.IndexService import IndexUtils
from dynamoplus.service.Utils import fromParametersToDict


class TestIndexService():

    def setUp(self):
        self.indexes = ["example#address.country__address.region__address.province__address.city__ORDER_BY__address.city","example#published__ORDER_BY__top","example#user_owner","category#active__ORDER_BY__ordering","category#objectId__ORDER_BY__ordering"]
        self.indexService = IndexUtils()
    
    def test_getValuesRecursively(self):
        expected={"address":{"country":"IT"}}
        result = fromParametersToDict({"address.country":"IT"})
        #result = dict(map(lambda kv: self.indexService._recursiveGet(kv[0],kv[1]) if "." in kv[0] else kv, {"address.country":"IT"}.items()))
        self.assertDictEqual(result,expected)
        expected={"published": "true"}
        result = fromParametersToDict({"published":"true"})
        #result = dict(map(lambda kv: self.indexService._recursiveGet(kv[0],kv[1]) if "." in kv[0] else kv, {"published":"true"}.items()))
        self.assertDictEqual(result,expected)
        expected={"address":{"country":"IT", "region": "Lombardia"}}
        result = fromParametersToDict({"address.country":"IT","address.region":"Lombardia"})
        #result = dict(map(lambda kv: self.indexService._recursiveGet(kv[0],kv[1]) if "." in kv[0] else kv, {"address.country":"IT","address.region":"Lombardia"}.items()))
        self.assertDictEqual(result,expected)
    def test_indexFromParameter(self):
        parameters={
            "published": "true",
            "orderBy": "top"
        }
        index = self.indexService.findIndexFromParameters("example",parameters)
        self.assertIsNotNone(index)
        self.assertEqual(index, self.indexes[1])
    def test_findIndexFromEntityCategory(self):
        entity={
            "id": "randomId",
            "objectId": "1241151",
            "ordering": "1",
            "name": "testname",
            "active": True
        }
        matchingIdexes = self.indexService.findIndexFromEntity(self.indexes,entity,"category")
        self.assertEqual(len(matchingIdexes),2, str(matchingIdexes))
    def test_getValuesFromEntityexample(self):
        entity={
            "id": "randomId",
            "published": True,
            "user_owner": "antessio7@gmail.com",
            "top": decimal.Decimal("10"),
            "latitude" : decimal.Decimal("45.433505"), 
            "longitude": decimal.Decimal("9.1784348"),
            "address":{
                'city': 'Milano', 
                'province': 'MI', 
                'region': 'Lombardia', 
                'country': 'IT'},
            "metadata": {
                "book2work": {
                    "category":{
                        "id": 1
                    }
                }
            }
        }
        values = self.indexService.getValuesFromEntity(self.indexes[0],entity)
        self.assertDictEqual(values, {"address.country": "IT","address.region":"Lombardia","address.province":"MI","address.city":"Milano"})
        values = self.indexService.getValuesFromEntity(self.indexes[1],entity)
        self.assertDictEqual(values, {"published": "true"})
        values = self.indexService.getValuesFromEntity(self.indexes[2],entity)
        self.assertDictEqual(values, {"user_owner": "antessio7@gmail.com"})
    def test_getValuesFromEntityCategory(self):
        entity={
            "id": "randomId",
            "objectId": "1241151",
            "ordering": "1",
            "name": "testname",
            "active": True
        }
        values = self.indexService.getValuesFromEntity(self.indexes[3],entity)
        self.assertDictEqual(values, {"active": "true"})
        values = self.indexService.getValuesFromEntity(self.indexes[4],entity)
        self.assertDictEqual(values, {"objectId": "1241151"})
    def test_findIndexFromEntity(self):
        entity={
            "id": "randomId",
            "published": True,
            "user_owner": "antessio7@gmail.com",
            "top": decimal.Decimal("10"),
            "latitude" : decimal.Decimal("45.433505"), 
            "longitude": decimal.Decimal("9.1784348"),
            "address":{
                'city': 'Milano', 
                'province': 'MI', 
                'region': 'Lombardia', 
                'country': 'IT'},
            "metadata": {
                "book2work": {
                    "category":{
                        "id": 1
                    }
                }
            }
        }
        matchingIdexes = self.indexService.findIndexFromEntity(self.indexes,entity,"example")
        self.assertEqual(len(matchingIdexes),3, str(matchingIdexes))
        #index 1
        self.assertEqual(matchingIdexes["example#published__ORDER_BY__top"]["conditions"], ["published"])
        self.assertEqual(matchingIdexes["example#published__ORDER_BY__top"]["values"], {"published":"true"})
        self.assertEqual(matchingIdexes["example#published__ORDER_BY__top"]["orderBy"], "top")
        self.assertEqual(matchingIdexes["example#published__ORDER_BY__top"]["orderByValue"], "10")
        #index 2
        self.assertEqual(matchingIdexes["example#user_owner"]["conditions"], ["user_owner"])
        self.assertEqual(matchingIdexes["example#user_owner"]["values"], {"user_owner":"antessio7@gmail.com"})
        self.assertNotIn("orderBy",matchingIdexes["example#user_owner"])
        #index 3
        self.assertEqual(matchingIdexes["example#address.country__address.region__address.province__address.city__ORDER_BY__address.city"]["conditions"], ["address.country","address.region","address.province","address.city"])
        self.assertEqual(matchingIdexes["example#address.country__address.region__address.province__address.city__ORDER_BY__address.city"]["values"], {"address.country":"IT","address.region":"Lombardia","address.province":"MI","address.city":"Milano"})
        self.assertIn("orderBy",matchingIdexes["example#address.country__address.region__address.province__address.city__ORDER_BY__address.city"])
        self.assertEqual(matchingIdexes["example#address.country__address.region__address.province__address.city__ORDER_BY__address.city"]["orderByValue"],"Milano")
    def test_findIndexFromEntity_keyNotFound(self):
            entity={
                "id": "randomId",
                "user_owner": "antessio7@gmail.com",
                "latitude" : decimal.Decimal("45.433505"), 
                "longitude": decimal.Decimal("9.1784348"),
                "metadata": {
                    "book2work": {
                        "category":{
                            "id": 1
                        }
                    }
                }
            }
            matchingIdexes = self.indexService.findIndexFromEntity(self.indexes,entity,"example")
            self.assertEqual(len(matchingIdexes),1)
            #index 1
            self.assertEqual(matchingIdexes["example#user_owner"]["conditions"], ["user_owner"])
            self.assertEqual(matchingIdexes["example#user_owner"]["values"], {"user_owner":"antessio7@gmail.com"})
            self.assertNotIn("orderBy",matchingIdexes["example#user_owner"])
    def test_dictDiffs(self):
        source={
            "id": "id",
            "value": "oldValue",
            "oldKey": "oldValue2"
        }
        target={
            "id": "id",
            "value": "newValue",
            "newKey": "newValue3"
        }
        dictWithDiff = self.indexService.dictDiffs(source,target)
        self.assertTrue("id" in dictWithDiff, "id is not in {}".format(str(dictWithDiff)))
        self.assertTrue("value" in dictWithDiff, "value is not in {}".format(str(dictWithDiff)))
        self.assertTrue("newKey" in dictWithDiff, "newKey is not in {}".format(str(dictWithDiff)))
        self.assertEqual("newValue",dictWithDiff["value"])
        self.assertEqual("id",dictWithDiff["id"])
        self.assertEqual("newValue3",dictWithDiff["newKey"])
