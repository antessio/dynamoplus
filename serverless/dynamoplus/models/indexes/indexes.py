import types


class Index(object):
    def __init__(self,entityName:str, indexName:str):
        self.entityName = entityName
        self.indexName = indexName
    @property 
    def indexName(self):
        return self.indexName
    @property
    def entityName(self):
        return self.entityName