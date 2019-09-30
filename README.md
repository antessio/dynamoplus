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

- `entity`
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
        "entity": {
            "name": "example
        },
        "name": "address.country__address.region__address.province__address.city__ORDER_BY__creation_date_time"
    }
    ```

To create a new entity: 

```
POST /dynamoplus/entity
{
    "name": "my-entity",
    "idKey": "objectId",
    "orderingKey": "custom_attribute"
}
```

Note: `custom_attribute` will be mandatory when creating a new row for `my-entity`

To create a new index:
```
POST /dynamoplus/index
{
    "entity": {
        "name": "example
        },
    "name": "address.country__address.region__address.province__address.city__ORDER_BY__creation_date_time"
}
```

# System Entities and indexes

