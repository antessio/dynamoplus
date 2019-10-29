import typing
import abc
import os

SYSTEM_ENTITIES = os.environ['ENTITIES']

class DynamoPlusHandler(abc.ABC):
    
    @abc.abstractmethod
    def get(self, collectionName: str, id:str):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        create repository
        4)
        get by id
        '''
        pass
    @abc.abstractmethod
    def create(self, collectionName: str, document:dict):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        validate
        4)
        create repository
        5)
        key generator
        6) 
        if exists => error
        7)
        create
        '''
        pass
    @abc.abstractmethod
    def update(self, collectionName: str, id:str, document:dict):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        validate
        4)
        create repository
        5)
        update
        '''
        pass
    @abc.abstractmethod
    def delete(self, collectionName:str, id:str):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        create repository
        4)
        delete
        '''        
        pass
    @abc.abstractmethod
    def query(self, collectionName:str, queryId:str, example:dict):
        '''
        1)
        domain:
            if collectioName not found system:
                raise NotFoundException
        system:
            no check
        2)
        get collection metadata:
            get from system
        3)
        create index service
        4)
        find by example
        '''
        pass
    @staticmethod
    def isSystem(collectionName):
        return collectionName in SYSTEM_ENTITIES.split(",")




