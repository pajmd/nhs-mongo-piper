import pymongo


class DbClient(object):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.close_db()
        else:
            self.close_db()
            raise

    def __init__(self, mongo_uri, mongo_db, collection_name, validate_schema=None, validation_schema=None):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.validate_schema = validate_schema
        self.validation_schema = validation_schema
        self.collection_name = collection_name
        self.connect_to_db()

    def connect_to_db(self):
        # An easy way to use thr DB is:
        #    client = MongoClient('%s:%d' % (MONGO_HOST, MONGO_PORT))
        #    collection = client.nhsdb.nhsCollection

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        if self.validate_schema:
            self.apply_validation()
        self.create_index("digest")

    def apply_validation(self):
        if self.collection_name in self.db.collection_names():
            self.db.runCommand({
                'collMod': self.collection_name,
                'validator': self.validation_schema
            }
            )
        else:
            self.db.createCollection(self.collection_name, **self.validation_schema)

    def close_db(self):
        self.client.close()

    def insert_documents(self, documents):
        operations = self.build_bulk_upsert(documents)
        rc = self.db[self.collection_name].bulk_write(operations)

    def build_bulk_upsert(self, documents):
        # UpdateOne({"field1": 11}, {"$set": {"field2": 12, "field3": 13 }}, upsert=True),
        operations = []
        for document in documents:
            mutating_document = document.copy()
            digest = mutating_document.pop("digest")
            if not self.mark_duplicate_document(document):
                operations.append(pymongo.UpdateOne({"digest": digest}, {"$set": mutating_document}, upsert=True))
        return operations

    def create_index(self, key):
        index_list = self.db[self.collection_name].list_indexes()
        truth = [index["key"].get(key) is None for index in index_list]
        if all(truth):
            self.db[self.collection_name].create_index(key)
            return True
        return False

    def mark_duplicate_document(self, document):
        digest = document['digest']
        docs = self.db[self.collection_name].find({"digest": digest})
        if docs.count():
            for doc in docs:
                if document['filename'] == 'full/2f307d3971227f3eaafcf9a6d5b7ca5b923be172.xlsx':
                    print('stop')
                if document['filename'] == doc['filename']:
                    print('there is a problem')

            file = {
                'url': document['url'],
                'filename': document['filename']
            }
            rc = self.db[self.collection_name].update_many(
                {"digest": digest},
                {"$push": {"dupes": file}})
            return True
        return False
