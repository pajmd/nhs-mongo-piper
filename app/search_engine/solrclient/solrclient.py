from app.search_engine.solrclient.httpclient import HttpClient
from app.search_engine.solrclient.solrclientexceptions import raise_for_status
from app.search_engine.solrclient.solrcommands import SolrOp as op


# https://lucene.apache.org/solr/guide/7_3/collections-api.html

# https://lucene.apache.org/solr/guide/7_6/uploading-data-with-index-handlers.html#solr-style-json

# indexing field definition https://lucene.apache.org/solr/guide/6_6/introduction-to-solr-indexing.html

# good read https://doc.lucidworks.com/lucidworks-hdpsearch/2.5/Guide-Solr.html

class SolrClient(HttpClient):
    def __init__(self, host, port, timeout=5):
        super(SolrClient, self).__init__(host, port, timeout)

    # curl -X POST -H 'Content-type:application/json'
    # --data-binary '{"add-field": {"name":"medicine", "type":"text_general", "multiValued":false, "stored":true}}'
    #  http://localhost:8983/solr/nhsdocs/schema
    # def add_field(self):
    #     pass

    #  /admin/collections?action=CREATE&name=name
    # replicationFactor=2&numSahrdPerNode=2&wt=json"
    # curl "http://localhost:8983/solr/admin/collections?
    # action=CREATE&name=nhsdocs&numShards=2&replicationFactor=2&maxShardsPerNode=2&wt=json"
    def create_collection(self, name, numshards, replication_factor, max_shards_per_node,
                          collection_configname='_default', optional=None):
        """
        Create a collection
        :param name:
        :param numshards:
        :param replication_factor:
        :param max_shards_per_node:
        :param collection_configname:
        :param optional: see https://lucene.apache.org/solr/guide/7_3/collections-api.html for extra parameters
        to be passed in a dictionary ex { 'tlogReplicas': 3 }
        :return:
        """
        command = 'solr/admin/collections'
        payload = {
            'action': 'CREATE',
            'name': name,
            'numShards': numshards,
            'replicationFactor': replication_factor,
            'maxShardsPerNode': max_shards_per_node,
            'collection.configName': collection_configname,
            'wt': 'json'
        }
        if optional:
            payload.update(optional)
        r = self.get(command, payload)
        raise_for_status(operation_type=op.ADMIN, operation=op.CREATE_COLLECTION, resp=r)

    # http://localhost:8983/solr/admin/collections?action=DELETE&name=newCollection&wt=xml
    def delete_collection(self, name):
        command = 'solr/admin/collections'
        payload = {
            'action': 'DELETE',
            'name': name,
            'wt': 'json'
        }
        r = self.get(command, payload)
        raise_for_status(operation_type=op.ADMIN, operation=op.DELETE_COLLECTION, resp=r)

    # curl -X POST -H 'Content-type:application/json'
    # --data-binary '{"add-field": {"name":"name", "type":"text_general", "multiValued":false, "stored":true}}'
    # http://localhost:8983/solr/films/schema
    def add_field(self, schema, name, fieldtype, multivalued, stored, optional=None):
        """
        Add a field to a collection i.e. a schema
        :param schema: collection name
        :param name:
        :param fieldtype:
        :param multivalued:
        :param stored:
        :param optional: see https://lucene.apache.org/solr/guide/7_6/defining-fields.html#defining-fields
        :return:
        """
        command = 'solr/%s/schema' % schema
        payload = {
            "add-field": {
                'name': name,
                'type': fieldtype,
                'multiValued': multivalued,
                'stored': stored
            }
        }

        if optional:
            payload["add-field"].update(optional)
        r = self.post(command, json_payload=payload)
        raise_for_status(operation_type=op.SCHEMA, operation=op.ADD_FIELD, resp=r)

    def add_fields(self, schema, fields):
        """
        Add a set of fields to a collection i.e. a schema

        ex:
        fields = [
            {
              "add-copy-field" : {
                "source":"*",
                "dest":"_text_"
              }
            },
            {
              "add-field":{
                 "name":"category",
                 "type":"text_general",
                 "multiValued":false,
                 "stored":true },
            },
            {
              "add-field":{
                 "name":"Formulations",
                 "type":"text_general",
                 "multiValued":true,
                 "stored":true },
            },
            {
              "add-field":{
                 "name":"Medicine",
                 "type":"text_general",
                 "multiValued":false,
                 "stored":true }
            },
        ]
        :param schema: collection name
        :param fields: a list of dict of several field commands or copy field commands...
        :return:
        """
        headers = {
            'Content-type': 'application/json'
        }
        command = 'solr/%s/schema' % schema
        r = self.post(command, fields=fields, header=headers)
        raise_for_status(operation_type=op.SCHEMA, operation=op.ADD_FIELD, resp=r)

    # curl -X POST -H 'Content-Type: application/json' 'http://localhost:8983/solr/my_collection/update/json/docs' --data-binary '
    # {
    #   "id": "1",
    #   "title": "Doc 1"
    # }'
    # single doc see https://lucene.apache.org/solr/guide/6_6/uploading-data-with-index-handlers.html#UploadingDatawithIndexHandlers-AddingMultipleJSONDocuments
    def add_document(self, collection, document):
        headers = {
            'Content-type': 'application/json'
        }
        command = 'solr/%s/update/json/docs' % collection
        r = self.post(command, json_payload=document, header=headers)
        raise_for_status(operation_type=op.INDEXING, operation=op.ADD_DOCUMENTS, resp=r)

    def add_documents(self, collection, documents, commit=True):
        headers = {
            'Content-type': 'application/json'
        }
        command = 'solr/%s/update?commit=%s' % (collection, ('true' if commit else 'false'))
        r = self.post(command, json_payload=documents, header=headers)
        raise_for_status(operation_type=op.INDEXING, operation=op.ADD_DOCUMENTS, resp=r)

    # curl 'http://localhost:8983/solr/techproducts/update?commit=true'
    #  --data-binary @example/exampledocs/books.json -H 'Content-type:application/json'
    def add_document_files(self, collection, files, commit=True):
        """
        indexes a file or a list of file (a file is made up of documents)
        :param collection:
        :param files:  a file or a list of file
        :param commit:
        :return: a single or a list of requests.Response()
        """
        command = 'solr/%s/update?commit=%s' % (collection, ('true' if commit else 'false'))
        header = {
            'Content-type': 'application/json'
        }
        r = self.post(command, files=files, header=header)

        raise_for_status(operation_type=op.INDEXING, operation=op.ADD_FILE, resp=r)
