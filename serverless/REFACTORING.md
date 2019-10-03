Starting from handler.py, self.getTargetConfiguration
- handler
- IndexService
- IndexRepository
- Repository


Configuration: 
- search document in system, if not found search in db
IndexService: 
- constructor with type Index
- is it possible to not pass dynamoDB and tableName but using decorator

IndexUtils:
- entityName and indexName or conditions in separate objects

# New structure
handlder->

    - search entity configuration, if not found 400
    - [for CRUD] instantiate a repository (or a service wrapping the repository) for the entity configuration
    - [for query] calls the index service to get the index corrisponding to the indexName in the path param
        - if the index has been found, execute the find by example on the indexService
    
configurationService ->

    - search for entity configuration in the system env, if not found search in the database using indexService and indexRepository as said before

indexService ->

    - wraps indexRepository

indexUtils ->

    - builds the sk and  data attributes, given some conditions
        - if incur in a composite condition find recursively the value

repository -> 

    - crud operation
    - find (optional?)

indexRepository ->

    - uses GSI for find
    - uses indexUtils(?) to generate the sk and data 