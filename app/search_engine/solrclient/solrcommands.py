#  python 3.4 >
from enum import Enum


class SolrOp(Enum):
    ADMIN = 1,
    CREATE_COLLECTION = 2
    DELETE_COLLECTION = 3
    SCHEMA = 10,
    ADD_FIELD = 11,
    INDEXING = 20,
    ADD_FILE = 21
    ADD_DOCUMENTS = 22

