master
[![Build Status](https://travis-ci.com/antessio/dynamoplus.svg?branch=master)](https://travis-ci.com/antessio/dynamoplus)


develop
[![Build Status](https://travis-ci.com/antessio/dynamoplus.svg?branch=develop)](https://travis-ci.com/antessio/dynamoplus)

master
[![Coverage Status](https://coveralls.io/repos/github/antessio/dynamoplus/badge.svg?branch=master)](https://coveralls.io/github/antessio/dynamoplus?branch=master)

develop
[![Coverage Status](https://coveralls.io/repos/github/antessio/dynamoplus/badge.svg?branch=develop)](https://coveralls.io/github/antessio/dynamoplus?branch=develop)

# dyamoplus
A serverless back-end to create REST endpoint in python

## Configuration

1. You must have Python 3! Once you do, run `pip install -r requirements.txt` to install Python dependencies

2. Install Docker. Why Docker? Because it's the only way to ensure that the Python package that is
   created on your local machine and uploaded to AWS will actually run in AWS's lambda containers. 

*NOTE: no authentication check is performed, the authorizer lambda is skipped 


## Installation

1. Configure your AWS credentials (see [Serverless framework AWS credentials](https://serverless.com/framework/docs/providers/aws/guide/credentials/))

2. Create a new file named secrets.json (you can copy `secrets-example.json` and edit accordingly with your configuration). 

3. run `sls deploy`

Once completed, you'll get the API gateway endpoint to call the API. 

By default you can access to all the API using the root account and password with basic authentication, then create a client_authorization (see documentation below) to create other clients.


# How it works

There are mainly two special entities:

- `collection`
    ```
    //example
    {
        "name": "example",
        "id_key": "id",
        "ordering": "creation_date_time"
    }
    ```
- `index`
    ```
    //example
    {
        "uid": <UUID>,
        "name": "field1__field2__ORDER_BY__field3",
        "collection": {
            "name": "example
        },
        "conditions":["field1","field2"],
        "ordering_key": "field2"
    }
    ```

To create a new collection: 

```
POST /dynamoplus/collection
{
    "name": "my-collection",
    "id_key": "objectId",
    "ordering": "custom_attribute"
}
```

Note: `custom_attribute` will be mandatory when creating a new row for `my-collection`

To create a new index:
```
POST /dynamoplus/index
{
    "collection": {
        "name": "example
        },
    "name": "field1__field2__ORDER_BY__field3",
    "conditions": ["field1","field2"],
    "ordering_key": "field3"
}
```

Once created some new endpoints will be available:
- `POST /dynamoplus/<collection-name>` to create a new document
- `GET /dynamoplus/<collection-name>/<id>` to get a document by its id
- `PUT /dynamoplus/<collection-name>/<id>` to update an existing document
- `DELETE /dynamoplus/<collection-name>/<id>` to delete a document by its id
- `POST /dynamoplus/<collection-name>/query` to query all documents in `collection-name`
- `POST /dynamoplus/<collection-name>/query/<index-name>` to query all documents in `collection-name` by `index-name`

## System collections

- **collection**: contains collections metadata
- **index**: contains indexes metadata
- **client_authorization**: contains all the authorized clients (used in authorization)


System collections have some special rules: 

- a system collection cannot be updated
- it is possibile to query the indexes only by collection name and queryAll
- it is not possible to execute queries on collections (except for queryAll)


## Queries

The query should match an index name, then accordingly with the fields declared in the index metadata, it will generate a key that identificates the dynamodb item. (see [DynamoDb query doc](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html) and [GSI overloading](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-gsi-overloading.html) for further info)

## Authorization

There are three authorization methods:

- basic auth : used with root username and password

- API key : used for trusted clients (it only checks an header and the client scope)

- http signature (raccomended): see [http-signature](https://tools.ietf.org/id/draft-cavage-http-signatures-08.html)


To create a client with API key:
```
POST /dynamoplus/client_authorization
{
    "client_id": "my-client-id",
    "client_scopes": [
        {
            "collection_name: "example",
            "scope_types": ["CREATE","UPDATE", "DELETE"]
        },
        {
            "collection_name: "person",
            "scope_types": ["QUERY", "GET"]
        }
        ],
        "api_key" : "secret"
}
```


To create a client with http signature:
```
POST /dynamoplus/client_authorization
{
    "client_id": "my-client-id",
    "client_scopes": [
        {
            "collection_name: "example",
            "scope_types": ["CREATE","UPDATE", "DELETE"]
        },
        {
            "collection_name: "person",
            "scope_types": ["QUERY", "GET"]
        }
        ],
        "client_public_key" : "-----BEGIN CERTIFICATE-----\nMIIDFTCCAf2gAwIBAgIJL5bSonccfwENMA0GCSqGSIb3DQEBCwUAMCgxJjAkBgNVBAMTHXdoaXRlbWFya2V0cGxhY2UuZXUuYXV0aDAuY29tMB4XDTE5MDkzMDE3MTU0OFoXDTMzMDYwODE3MTU0OFowKDEmMCQGA1UEAxMdd2hpdGVtYXJrZXRwbGFjZS5ldS5hdXRoMC5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDBPIF8fgB7dpj6lVVOH2IYUKv02S54OztaaNIzjLrUfAQrzQAW4DmuZFkqWKxgyi5zINsEm9qxgj8uYOVhaWUWve7QaREjSPF5vNZonsLswfCci9U1JqEUdbDJQmoWPKXav0olMwXQV8W9hTnI2Gmi+8qBVIi18jUpXY/0OApsOTyQo51wd+EvRkDYeOIeqY7A/qbWEvNK9xWO1ainmZV5jc4vEsH2wMfpdXA+28LFhg/VeLKk7Zzaa46T5AdRRUm3V6DWLvujwh5Tfo3KjWnPq8KGWsJuOyMevqe5ESNfEIfiL33n0XJC8oAcfoqfVvYbyzjGjC40OXXoN48wTAgMBAAGjQjBAMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYEFG6Hul2iHizqBE6BHJ8/ldZQAu8+MA4GA1UdDwEB/wQEAwIChDANBgkqhkiG9w0BAQsFAAOCAQEAEXuc0uxGXS/NVfc22O3wWT9m0GGogMPpmZae7BXtM6D5wkwBdElOlZky6QePfyv73HzdibkjinnA174xNyqiVSS3mr0bmgyhv78undBFgN0Bsx4m3Bm4nwMWvNRsR0IZeNAM8Kx469YOBtkzmeNf8SqHdwApUx7vceZNIywvzB2doihvuSvs3kOreBCMp72hpLwZ646LyLuqD2B7Ll3huZtdJ8tbjRqiCtrBmDfv9BSo81VlyA4yDIq8PTcFGkvrU0mLkiliU5lrLcGgxNFcL5TnJ1dtvHualIu6s2L091aKlOMNMMyMsw+wzhRTkaqFqYaw9P8an9C0D/e3g==\n-----END CERTIFICATE-----"
}
```


## Examples

### Creating a book store

- Create a client book-store-client (ADMIN)
```
POST /dynamoplus/client_authorization
Headers:
    Authorization: base64(username:password)
Body:
{
    "client_id": "book-store-client",
    "client_scopes": [
        {
            "collection_name: "book",
            "scope_types": [
                "CREATE",
                "UPDATE", 
                "DELETE",
                "QUERY", 
                "GET"]
        },
        {
            "collection_name: "category",
            "scope_types": [
                "CREATE",
                "UPDATE", 
                "DELETE",
                "QUERY", 
                "GET"]
        }],
        "api_key" : "secret1234"
}
```

- Create a book category collection (ADMIN)

    ```
    POST /dynamoplus/collection
    {
        "name": "category",
        "id_key": "id",
        "ordering": "ordering"
    }
    ```

- Create a book collection (ADMIN)

    ```
    POST /dynamoplus/collection
    {
        "name": "book",
        "id_key": "id",
        "ordering": "rating"
    }
    ```
- Create a some indexes on book (ADMIN)
    
    ```
    POST /dynamoplus/index (book by title)
    {
        "collection": {
            "name": "book"
        },
        "name": "title"
    }
    ```
    ```
    POST /dynamoplus/index (book by isbn)
    {
        "collection": {
            "name": "book"
        },
        "name": "isbn",
        "fields": ["isbn"]
    }
    ```
    ```
    POST /dynamoplus/index (book by category name)
    {
        "collection": {
            "name": "book"
        },
        "name": "category.name",
        "fields": ["category.name"]
    }
    ```
    ```
    POST /dynamoplus/index (book by author)
    {
        "collection": {
            "name": "book"
        },
        "name": "author",
        "fields": ["author"]
    }
    ```

- For CLIENT request use the following headers:
    ```
    Authorization: dynamoplus-api-key secret1234
    dynamoplus-client-id: book-store-client
    ``` 
- Query the indexes just created (CLIENT)
    ```
    POST /dynamoplus/index/query/collection.name
    {
        "matches":{
            "collection":{
                "name": "book"
            }
        }
    }
    ```
    **Response**
    ```
    {
        "data": [
            {
            "creation_date_time": "2019-10-02T13:51:14.953604",
            "collection": {
                "name": "book"
            },
            "id": "b37a7ea8-e51b-11e9-ac9d-12cf86817cbb",
            "name": "isbn"
            },
            {
            "creation_date_time": "2019-10-02T13:51:25.956478",
            "collection": {
                "name": "book"
            },
            "id": "ba0962f2-e51b-11e9-ac9d-12cf86817cbb",
            "name": "category.name"
            },
            {
            "creation_date_time": "2019-10-02T13:52:07.315643",
            "collection": {
                "name": "book"
            },
            "id": "d2b04b86-e51b-11e9-ac9d-12cf86817cbb",
            "name": "title"
            },
            {
            "creation_date_time": "2019-10-02T13:51:20.053744",
            "collection": {
                "name": "book"
            },
            "id": "b684b424-e51b-11e9-ac9d-12cf86817cbb",
            "name": "author"
            }
        ],
        "lastKey": null
        }
    ```

- Create a new category (CLIENT)
    ```
    POST /dynamoplus/category
    {
        "name": "Pulp",
        "ordering": "1"
    }
    ```
- Create some books (CLIENT)
    ```
    POST /dynamoplus/book
    {
        "isbn": "11121421",
        "author": "Chuck Palhaniuk",
        "title": "Fight Club",
        "category": {
            "id": "1",
            "name": "Pulp"
        },
        "rating": "10"
    }
    ```
    ```
    POST /dynamoplus/book
    {
        "isbn": "11121421",
        "author": "Chuck Palhaniuk",
        "title": "Choke",
        "category": {
            "id": "1",
            "name": "Pulp"
        },
        "rating": "9"
    }
    ```
    ```
    POST /dynamoplus/book
    {
        "isbn": "11121421",
        "author": "Chuck Palhaniuk",
        "title": "Haunted",
        "category": {
            "id": "1",
            "name": "Pulp"
        },
        "rating": "8"
    }
    ```
- Query books by author (CLIENT)
    ```
    POST /dynamoplus/book/query/author
    {
        "matches":{
            "author": "Chuck Palhaniuk"
        }
    }
    ```
- Query books by category name (CLIENT)
    ```
    POST /dynamoplus/book/query/category.name
    {
        "matches": {
            "category": {
                "name": "Pulp"
            }
        }
    }
    ```
- Query books by title (CLIENT)
    ```
    POST /dynamoplus/book/query/title
    {
        "matches: {
            "title": "Fight Club"
        }
    }
    ```