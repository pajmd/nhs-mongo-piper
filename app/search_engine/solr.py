from app.settings import SOLR_CLIENT, SOLR_PORT, COLLECTION_NAME
from app.search_engine.solrclient.solrclient import SolrClient


def send(records):
    client = SolrClient(host=SOLR_CLIENT, port=SOLR_PORT)
    client.add_documents(COLLECTION_NAME, records)