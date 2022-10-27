
# aggregations dynamo db table


- for MAX and MIN: an index by the field must be present so if it doesn't exist, then create it and make it unmodifiable
    - in the API or whatever may change the index, the update must be forbidden
    - aggregate function should just check if the index is present, if not create it
    - to query the value of min max: ensure that the is possible to query by the field ordered by the field itself (desc/asc)
- for count and sum: they are strictly connected, if avg is defined then sum is automatically created
    - to safe store the values the atomic increment should be performed, that's why the aggregation must have
    three different rows
    - to query the index by count just query aggregation#$ID and extract count 

## system collection metadata

| pk                                         | sk                                        | data             |
|--------------------------------------------|-------------------------------------------|------------------|
| aggregation_configuration#$ID              | aggregation_configuration                 | $ID              |
| aggregation_configuration#$ID              | aggregation_configuration#collection_name | $collection-name |
| collection#$collection_name                | collection                                | $collection-name |
| collection_document_count#$collection_name | collection_document_count                 |                  |
| aggregation_count#$ID                      | $aggregation_name                         | $value           |
| aggregation_sum#$ID                        | $aggregation_name                         | $value           |

## domain data


| pk                   | sk               | data |
|----------------------|------------------|------|
| $collection_name#$ID | $collection_name | $ID  |

## examples

**System**

| pk                                   | sk                                        | data       | json                                                                                                                                                                        |
|--------------------------------------|-------------------------------------------|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| collection#restaurant                | collection                                | restaurant | {"name": "restaurant", "id_key": "id", "auto_generate_id": true}                                                                                                            |
| collection#review                    | collection                                | review     | {"name": "review", "id_key": "id", "auto_generate_id": true}                                                                                                                |
| collection#booking                   | collection                                | review     | {"name": "booking", "id_key": "id", "auto_generate_id": true}                                                                                                               |
| collection_document_count#restaurant | collection_document_count                 | 4          |                                                                                                                                                                             |
| collection_document_count#review     | collection_document_count                 | 6          |                                                                                                                                                                             |
| collection_document_count#booking    | collection_document_count                 | 8          |                                                                                                                                                                             |
| aggregation_configuration#1          | aggregation_configuration                 | 1          | {"collection":{"name":"review"},"type":"AVG","aggregation":{"on":["INSERT"],"target_field":"rate"}}                                                                         |
| aggregation_configuration#1          | aggregation_configuration#collection.name | review     |                                                                                                                                                                             |
| aggregation#1                        | aggregation                               | 1          | {"sum":35, "count": 6}                                                                                                                                                      |
| aggregation_configuration#2          | aggregation_configuration                 | 2          | {"collection":{"name":"restaurant"},"type":"AVG","aggregation":{"on":["INSERT"],"target_field":"seat","matches":{"and":[{"eq":{"field_name":"type","value":"pizzeria"}}]}}} |
| aggregation_configuration#2          | aggregation_configuration#collection.name | restaurant |                                                                                                                                                                             |
| aggregation#2                        | aggregation                               | 2          | {"sum": 430,  "count": 3}                                                                                                                                                   |

**Domain**

| pk           | sk                    | data | json                                 |
|--------------|-----------------------|------|--------------------------------------|
| restaurant#1 | restaurant            | 1    | {"type": "pizzeria","seat": 300}     |
| restaurant#2 | restaurant            | 2    | {"type": "pizzeria","seat": 100}     |
| restaurant#3 | restaurant            | 3    | {"type": "greek","seat": 20}         |
| restaurant#4 | restaurant            | 4    | {"type": "pizzeria","seat": 30}      |
| review#1     | review                | 1    | {"rate": 10,"restaurant_id": "1"}    |
| review#1     | review#restaurant_id  | 1    |                                      |
| review#2     | review                | 2    | {"rate": 3,"restaurant_id": "1"}     |
| review#2     | review#restaurant_id  | 1    |                                      |
| review#3     | review                | 3    | {"rate": 9,"restaurant_id": "1"}     |
| review#3     | review#restaurant_id  | 1    |                                      |
| review#11    | review                | 11   | {"rate": 1,"restaurant_id": "2"}     |
| review#11    | review#restaurant_id  | 2    |                                      |
| review#21    | review                | 21   | {"rate": 5,"restaurant_id": "2"}     |
| review#21    | review#restaurant_id  | 2    |                                      |
| review#31    | review                | 31   | {"rate": 8,"restaurant_id": "2"}     |
| review#31    | review#restaurant_id  | 2    |                                      |
| booking#1    | booking               | 1    | {"amount": 300,"restaurant_id": "1"} |
| booking#1    | booking#restaurant_id | 1    |                                      |
| booking#2    | booking               | 2    | {"amount": 40,"restaurant_id": "1"}  |
| booking#2    | booking#restaurant_id | 1    |                                      |
| booking#3    | booking               | 3    | {"amount": 50,"restaurant_id": "1"}  |
| booking#3    | booking#restaurant_id | 1    |                                      |
| booking#4    | booking               | 4    | {"amount": 40,"restaurant_id": "1"}  |
| booking#4    | booking#restaurant_id | 1    |                                      |
| booking#11   | booking               | 11   | {"amount": 300,"restaurant_id": "2"} |
| booking#11   | booking#restaurant_id | 2    |                                      |
| booking#21   | booking               | 21   | {"amount": 40,"restaurant_id": "2"}  |
| booking#21   | booking#restaurant_id | 2    |                                      |
| booking#31   | booking               | 31   | {"amount": 50,"restaurant_id": "2"}  |
| booking#31   | booking#restaurant_id | 2    |                                      |
| booking#41   | booking               | 41   | {"amount": 40,"restaurant_id": "2"}  |
| booking#41   | booking#restaurant_id | 2    |                                      |



