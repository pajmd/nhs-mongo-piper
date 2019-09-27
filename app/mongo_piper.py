from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from app.settings import (
    TOPIC, GROUP_ID, KAFKA_BROKERS,
    MONGO_URI, MONGO_DATABASE, COLLECTION_NAME
)
from app.db.store import DbClient
from app.search_engine import solr

MAX_RECORDS = 10
MAX_POLL_TIME = 1000  # 1 sec


def consume_pipe():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
#        auto_offset_reset='earliest',
        enable_auto_commit=False,
        max_poll_records=MAX_RECORDS,
        # consumer_timeout_ms=10,  # ms
        group_id=GROUP_ID,
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    while(True):
        try:
            records = consumer.poll(MAX_POLL_TIME, MAX_RECORDS)
            try:
                with DbClient(MONGO_URI, MONGO_DATABASE, COLLECTION_NAME) as db:
                    db.insert_documents(records)
                solr.send(records)
                consumer.commit()
            except Exception as ex:  # create exception DB and solr specific
                print('Error while storing data: %s' % str(ex))
        except Exception as ex:  # create exception DB and solr specific
            print('Error while polling: %s' % str(ex))
        finally:
            print('Closing db consumer')
            consumer.close()
            break


if __name__ == "__main__":
    consume_pipe()
