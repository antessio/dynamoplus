# Dynamoplus

## Data model and classes

````puml
@startuml
title Data model

abstract class ClientAuthorization{
- client_id:String
- client_scopes:List<ClientScope> 

}

class ClientScope{
- scope_type: ScopeType
- collection: Collection
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
class Index{
- collection: Collection
- conditions: List<String>
- index_configuration: IndexConfiguration
- ordering_key: String
}


ClientAuthorizationHttpSignature--|>ClientAuthorization
ClientAuthorizationApiKey--|>ClientAuthorization
ClientAuthorization--*ClientScope
ClientScope "collection_name"--*Collection
Collection--->AttributeDefinition
Index "collection_name"--*Collection
@enduml
````

````puml
title Services

class Model{
- pk:String
- sk:String
- data:String
- document: dict
}

class QueryResult{
- last_evaluated_key: dict
- data: List<Model>
}

abstract class Repository{
+ create(model:Model): Model
+ get(pk:String,sk:String):Model
+ increment_counter(atomic_increment:AtomicIncrement)
+ update(model:Model):Model
+ delete(pk:String,sk:String)
}
class DomainRepository
class SystemRepository

class QueryRepository{

- query_gsi(key:Key, limit:Integer, last_key:Model)
+ query_begins_with(sk:String, data:String, last_key:Model, limit:Integer):QueryResult
+ query_gt(sk:String, data:String, last_key:Model, limit:Integer):QueryResult
+ query_lt(sk:String, data:String, last_key:Model, limit:Integer):QueryResult
+ query_all(sk:String, last_key:Model, limit:Integer):QueryResult
+ query_range(sk:String, from_data:String, to_data:String last_key:Model, limit:Integer):QueryResult
}

class QueryService{
 
}
class CollectionService{

+ get_collection(collection_name:String):Collection
+ create_collection(collection:Collection):Collection
+ delete_collection(collection_name:String):void
+ get_all_collections(starting_from:String, limit:Integer):List<Collection>
+ get_all_collections_generator(starting_from:String, limit:Integer):Stream<Collection>
}

class IndexService{

+ get_index_by_name(name:String):Index
+ create_index(index:Index): Index
+ get_index_by_name_and_collection_name(name:String, collection_name:String): List<Index>
+ get_index_by_collection_name(collection_name:String, start_from:String, limit:Integer):List<Index>
+ get_index_matching_fields(fields:List<String>, collection_name:String, ordering_key:String):List<Index>
+ delete_index(name:String):void
+ get_indexes_from_collection_name_generator(collection_name:String):Stream<Index>

}
class AuthorizationService{


}


DomainRepository--|>Repository
SystemRepository--|>Repository
QueryResult-->Model
CollectionService--*SystemRepository
IndexService--*SystemRepository
AuthorizationService--*SystemRepository
QueryService--*QueryRepository
CollectionService--*QueryService
IndexService--*QueryService
AuthorizationService--*QueryService
````
````puml
title TODO

abstract class Repository{
+ create(model:Model): Model
+ get(pk:String,sk:String):Model
+ update(model:Model):Model
+ delete(pk:String,sk:String)
+ query_begins_with(partition_key:String, sort_key:String, last_key:Model, limit:Integer, index:String=None):QueryResult
+ query_gt(partition_key:String, sort_key:String,, last_key:Model, limit:Integer, index:String=None):QueryResult
+ query_lt(partition_key:String, sort_key:String,, last_key:Model, limit:Integer, index:String=None):QueryResult
+ query_all(partition_key:String, last_key:Model, limit:Integer, index:String=None):QueryResult
+ query_range(partition_key:String, from_sort_key:String, to_sort_key:String last_key:Model, limit:Integer, index:String=None):QueryResult
+ increment_counter(atomic_increment:AtomicIncrement)
- query_gsi(key:Key, limit:Integer, last_key:Model)
}


class DomainRepository
class SystemRepository


class IndexService{

+ get_index_by_name(name:String):Index
+ create_index(index:Index): Index
+ get_index_by_name_and_collection_name(name:String, collection_name:String): List<Index>
+ get_index_by_collection_name(collection_name:String, start_from:String, limit:Integer):List<Index>
+ get_index_matching_fields(fields:List<String>, collection_name:String, ordering_key:String):List<Index>
+ delete_index(name:String):void
+ get_indexes_from_collection_name_generator(collection_name:String):Stream<Index>

}
class AuthorizationService{

}

class CollectionService{
+ get_collection(collection_name:String):Collection
+ create_collection(collection:Collection):Collection
+ delete_collection(collection_name:String):void
+ get_all_collections(starting_from:String, limit:Integer):List<Collection>
+ get_all_collections_generator(starting_from:String, limit:Integer):Stream<Collection>
}

class DomainService{
+ get_document(id:String, collection:Collection):dict
+ create_document(collection:Collection, document:dict):dict
+ delete_document(collection:Collection, document:dict):void
+ get_all_documents(collection:Collection,starting_from:String, limit:Integer):List<dict>
+ query_document(predicate:Predicate, starting_from:String, limit:Integer):List<dict>
}

DomainService--*DomainRepository
DomainRepository--|>Repository
SystemRepository--|>Repository
CollectionService--*SystemRepository
IndexService--*SystemRepository
AuthorizationService--*SystemRepository
````
