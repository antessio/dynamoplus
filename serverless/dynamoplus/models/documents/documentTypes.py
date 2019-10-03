import typing

class DocumentTypeConfiguration(object):
    def __init__(self, entityName:str, idKey:str, orderingKey:str):
        self.entityName = entityName
        self.idKey = idKey
        self.orderingKey = orderingKey
    def entityName(self):
        return self.entityName
    def idKey(self):
        return self.idKey
    def orderingKey(self):
        return self.orderingKey
