class DocumentTypeConfiguration(object):
    def __init__(self, entity_name: str, id_key: str, ordering_key: str):
        self.entityName = entity_name
        self.idKey = id_key
        self.orderingKey = ordering_key

    @property
    def entity_name(self):
        return self.entity_name

    @property
    def id_key(self):
        return self.id_key

    @property
    def ordering_key(self):
        return self.ordering_key
