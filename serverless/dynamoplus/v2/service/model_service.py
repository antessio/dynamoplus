import logging
from typing import *

from dynamoplus.models.system.index.index import Index
from dynamoplus.models.system.collection.collection import Collection
from dynamoplus.utils.utils import get_values_by_key_recursive, convert_to_string
from dynamoplus.v2.repository.repositories import Model

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_model(collection: Collection, document: dict):
    if collection.id_key not in document:
        raise Exception("{} not found in document".format(collection.id_key))
    id = document[collection.id_key]
    return Model("{}#{}".format(collection.name, id),
                 collection.name,
                 document[collection.ordering_key] if collection.ordering_key in document else id,
                 document
                 )


def get_index_model(collection:Collection, index: Index, document: dict):
    def build_data():
        logging.info("orderKey {}".format(index.ordering_key))
        order_value = None
        try:
            order_value = document[index.ordering_key] \
                if index.ordering_key is not None and index.ordering_key in document \
                else None
        except AttributeError:
            logging.debug("ordering key missing")
        logging.debug("orderingPart {}".format(order_value))
        logging.info("Entity {}".format(str(document)))

        logging.info("Index keys {}".format(index.conditions))
        '''
            attr1#attr2#attr3#attr4#orderValue
        '''
        values = get_values_by_key_recursive(document, index.conditions)
        logging.info("Found {} in conditions ".format(values))

        if values:
            data = "#".join(list(map(lambda v: convert_to_string(v), values)))
            if order_value:
                data = data + "#" + order_value
            return data

    sk = index.collection_name + "#" + \
         "#".join(map(lambda x: x, index.conditions)) if index.conditions else index.collection_name
    data = build_data()
    return Model(get_pk(collection,document[collection.id_key]), sk, data, document)


def get_sk(collection: Collection):
    return collection.name


def get_pk(collection: Collection, id):
    return "{}#{}".format(collection.name, id)
