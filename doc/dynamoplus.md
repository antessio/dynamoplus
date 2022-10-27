# Dynamoplus


````puml
@startuml
abstract class ClientAuthorization{
- client_id:String
- client_scopes:List<ClientScope> 

}

class ClientScope{
- scope_type: ScopeType
- collection_name: String
}

class ClientAuthorizationApiKey{
- api_key:String
- whitelist_hosts: List<String>
}
class ClientAuthorizationHttpSignature{
- client_public_key: String
}

class Collection{
- name: String
- id_key: String
- attributes: List<AttributeDefinition>
- ordering_key: String
- auto_generated_id: Boolean
}

class AttributeDefinition{
- name: String
- type: AttributeType
}

ClientAuthorizationHttpSignature--|>ClientAuthorization
ClientAuthorizationApiKey--|>ClientAuthorization
ClientAuthorization--*ClientScope
@enduml
````


```puml
title Client creation


participant "Client" as c
participant "Api Gateway" as api
participant "Lambda" as l
participant "DynamoStreamHandler" as dsh


c->api: POST /client_authorization\nAuthorization: <root>\nscope: READ_WRITE\ncollection_name: <collection_name>
api->l:
activate l
l->l: create client_authorization\non system table
l->api: client_authorization
deactivate l
api->c: 200
```

````puml

title Collection creation


participant "Client" as c
participant "Api Gateway" as api
participant "Lambda" as l
participant "DynamoStreamHandler" as dsh


c->api: POST /collection\nAuthorization: <root>
api->l:
activate l
l->l: create collection "restaurant"\non system table
l->api: collection_metadata
deactivate l
api->c: 200
````