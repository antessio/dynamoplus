https://travis-ci.com/antessio/dynamoplus.svg?branch=feature%2FrefactoringDynamicEntityAndIndex


# dyamoplus
A serverless back-end to create REST endpoint in python

## Configuration

1. You must have Python 3! Once you do, run `pip install -r requirements.txt` to install Python web token dependencies

2. Install Docker. Why Docker? Because it's the only way to ensure that the Python package that is
   created on your local machine and uploaded to AWS will actually run in AWS's lambda containers. 

2. Setup an [auth0 client](https://auth0.com/docs/clients) and get your `client id` and `client secrets` from auth0.

3. Plugin your `AUTH0_CLIENT_ID`, `AUTH0_CLIENT_SECRET` and `AUDIENCE` in a new file called `secrets.json`. These will be used by the JSON web token decoder to validate private api access.

4. Copy the `public_key_example` file to a new file named `public_key` and follow the instructions in that file

# How it works

There are two special entities:

- `document_type`
    ```
    //example
    {
        "name": "example",
        "idKey": "id",
        "orderingKey": "creation_date_time"
    }
    ```
- `index`
    ```
    //example
    {
        "document_type": {
            "name": "example
        },
        "name": "address.country__address.region__address.province__address.city__ORDER_BY__creation_date_time"
    }
    ```

To create a new document_type: 

```
POST /dynamoplus/document_type
{
    "name": "my-document_type",
    "idKey": "objectId",
    "orderingKey": "custom_attribute"
}
```

Note: `custom_attribute` will be mandatory when creating a new row for `my-document_type`

To create a new index:
```
POST /dynamoplus/index
{
    "document_type": {
        "name": "example
        },
    "name": "address.country__address.region__address.province__address.city__ORDER_BY__creation_date_time"
}
```

Once created some new endpoints will be available:
- `POST /dynamoplus/<document_type-name>` to create a new document

## Examples

### Creating a book store

- Create a book category document type

    ```
    POST /dynamoplus/document_type
    {
        "name": "category",
        "idKey": "id",
        "orderingKey": "ordering"
    }
    ```

- Create a book document type

    ```
    POST /dynamoplus/document_type
    {
        "name": "book",
        "idKey": "id",
        "orderingKey": "rating"
    }
    ```
- Create a some indexes on book
    
    ```
    POST /dynamoplus/index (book by title)
    {
        "document_type": {
            "name": "book"
        },
        "name": "title"
    }
    ```
    ```
    POST /dynamoplus/index (book by isbn)
    {
        "document_type": {
            "name": "book"
        },
        "name": "isbn"
    }
    ```
    ```
    POST /dynamoplus/index (book by category name)
    {
        "document_type": {
            "name": "book"
        },
        "name": "category.name"
    }
    ```
    ```
    POST /dynamoplus/index (book by author)
    {
        "document_type": {
            "name": "book"
        },
        "name": "author"
    }
    ```
- Query the indexes just created
    ```
    POST /dynamoplus/index/query/document_type.name
    {
	    "document_type":{
		    "name": "book"
	    }
    }
    ```
    **Response**
    ```
    {
        "data": [
            {
            "creation_date_time": "2019-10-02T13:51:14.953604",
            "document_type": {
                "name": "book"
            },
            "id": "b37a7ea8-e51b-11e9-ac9d-12cf86817cbb",
            "name": "isbn"
            },
            {
            "creation_date_time": "2019-10-02T13:51:25.956478",
            "document_type": {
                "name": "book"
            },
            "id": "ba0962f2-e51b-11e9-ac9d-12cf86817cbb",
            "name": "category.name"
            },
            {
            "creation_date_time": "2019-10-02T13:52:07.315643",
            "document_type": {
                "name": "book"
            },
            "id": "d2b04b86-e51b-11e9-ac9d-12cf86817cbb",
            "name": "title"
            },
            {
            "creation_date_time": "2019-10-02T13:51:20.053744",
            "document_type": {
                "name": "book"
            },
            "id": "b684b424-e51b-11e9-ac9d-12cf86817cbb",
            "name": "author"
            }
        ],
        "lastKey": null
        }
    ```

- Create a new category
    ```
    POST /dynamoplus/category
    {
        "name": "Pulp",
        "ordering": "1"
    }
    ```
- Create some books
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
- Query books by author
    ```
    POST /dynamoplus/book/query/author
    {
        "author": "Chuck Palhaniuk"
    }
    ```
- Query books by category name
    ```
    POST /dynamoplus/book/query/category.name
    {
        "category": {
            "name": "Pulp"
        }
    }
    ```
- Query books by title
    ```
    POST /dynamoplus/book/query/title
    {
        "title": "Fight Club"
    }
    ```