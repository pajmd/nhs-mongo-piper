from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from app.settings import (
    TOPIC, MONGO_GROUP_ID, KAFKA_BROKERS,
    MONGO_URI, MONGO_DATABASE, COLLECTION_NAME
)
from app.db.store import DbClient
from app.search_engine import solr
import time
import logging


logger = logging.getLogger(__name__)

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
        group_id=MONGO_GROUP_ID,
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    while(True):
        try:
            nhs_records = []
            records = consumer.poll(MAX_POLL_TIME, MAX_RECORDS)
            for topic_partition, consumer_records in records.items():
                logger.debug('topic: %s, partition=%d' % (topic_partition.topic, topic_partition.partition))
                logger.debug(consumer_records)
                for record in consumer_records:
                    logger.debug(record.offset)
                    nhs_records.append(record.value['doc'])
            try:
                with DbClient(MONGO_URI, MONGO_DATABASE, COLLECTION_NAME) as db:
                    db.insert_documents(nhs_records)

                def make_solr_record(nhs_record):
                    nhs_record['ns'] = COLLECTION_NAME
                    nhs_record['_ts'] = time.time()
                    return nhs_record

                solr_nhs_records=[make_solr_record(nhs_record) for nhs_record in nhs_records]
                solr.send(solr_nhs_records)
                consumer.commit()
            except Exception as ex:  # create exception DB and solr specific
                logger.debug('Error while storing data: %s' % str(ex))
        except KafkaConsumer.WakeupException as ex:
            # shutdown hook woke up poll with consumer.wakeup()
            consumer.close()
            break
        except Exception as ex:  # create exception DB and solr specific
            logger.debug('Error while polling: %s' % str(ex))
            logger.debug('Closed consumer')
            time.sleep(5)


def main():
    # logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', filename='stdout', level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    consume_pipe()


if __name__ == "__main__":
    main()
