import typing

class DocumentTypeConfiguration(object):
    def __init__(self, entityName:str, idKey:str, orderingKey:str):
        self.entityName = entityName
        self.idKey = idKey
        self.orderingKey = orderingKey
    @property
    def entityName(self):
        return self.entityName
    @property
    def idKey(self):
        return self.idKey
    @property
    def orderingKey(self):
        return self.orderingKey
