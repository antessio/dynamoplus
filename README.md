# dynamoplus

[![Coverage Status](https://coveralls.io/repos/github/antessio/dynamoplus/badge.svg?branch=master)](https://coveralls.io/github/antessio/dynamoplus?branch=master) ![Integration tests ](https://github.com/antessio/dynamoplus/workflows/Integration%20tests/badge.svg?branch=master)


Dynamoplus is a serverless back-end written in python and based on AWS DynamoDB.

The goal is to provide a REST APIs to easily access to dynamodb and to hide the "GSI overloading" implementation([for further details](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-gsi-overloading.html)).
 
## Main features

- create collections
- store documents in a collection (CRUD API)
- limit the access to your collections only to given clients
- define access patterns to query documents
- configure aggregations and perform aggregation queries (count, max, avg)


![](dynamoplus.png)



## Related Projects

- [dynamoplus admin dashboard](https://github.com/antessio/dynamoplus-admin-dashboard): a web app to easily access to dynamoplus through a web interface. **Work in progress**. 

- [~~dynamoplus python sdk~~](https://github.com/antessio/dynamoplus-python-sdk): a python sdk to access to dynamoplus API (**Currently not mantained it was implemented on version 0.1**.

- [dynamoplus end-to-end tests](https://github.com/antessio/dynamoplus-e2e-tests): java junit project to run end to end tests
on dynamoplus APIs

- [dynamoplus Java sdk](https://github.com/antessio/dynamoplus-java-sdk): java sdk to call the API
