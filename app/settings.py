import os


# Kafka
# pipe
TOPIC = 'scrapypipe'
MONGO_GROUP_ID = 'mongo_feeder'
SOLR_GROUP_ID = 'solr_feeder'
KAFKA_HOST = os.environ.get('KAFKA_HOST', 'localhost')
KAFKA_BROKERS = ['%s:9092' % KAFKA_HOST]

# MONGO
MONGO_HOST = os.environ.get('MONGO_HOST','localhost')
MONGO_PORT = os.environ.get('MONGO_PORT',27017)

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://%s:%d/' % (MONGO_HOST, MONGO_PORT))
MONGO_DATABASE = os.environ.get('MONGO_DATABASE', 'nhsdb')
COLLECTION_NAME = os.environ.get('COLLECTION_NAME', 'nhsCollection')
VALIDATE_SCHEMA =  os.environ.get('VALIDATE_SCHEMA', False)

VALIDATION_SCHEMA =  {
    'validator': {
        '$jsonSchema': [
            {'bsonType': "object"},
            {'required': ["category", "Formulations", "Medicine", "unit", "period", "Pack_Size", "VMPP_Snomed_Code",
                          "Basic_Price"]},
            {'properties': {
                'Medicine': [
                    ('bsonType', "string"),
                    ('description', "must be a string and is required")
                ],
                'Basic_Price': [
                    ('bsonType', "float"),
                    ('description', "must be a float and is required")
                ]
            }
            }
        ]
    }
}

# SOLR
SOLR_CLIENT = os.environ.get('SOLR_CLIENT', 'localhost')
SOLR_PORT = os.environ.get('SOLR_PORT', 8983)
