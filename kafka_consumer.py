from time import sleep
import datetime, logging, json, os, requests, ssl, sys
from pymongo import MongoClient, UpdateOne, DeleteOne, TEXT
from pymongo.errors import BulkWriteError
import pandas as pd
import dateutil.parser as parser
from bson import ObjectId
from json import loads
import io
from confluent_kafka import avro, TopicPartition
from confluent_kafka.avro import AvroConsumer
import logging
import sys
import commonmark
import lxml.html
from multiprocessing import Process
from confluent_kafka.avro.serializer import SerializerError

log = logging.getLogger(__name__)
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setLevel(logging.INFO)
log.addHandler(out_hdlr)
log.setLevel(logging.INFO)

ssl.match_hostname = lambda cert, hostname: True
mytopicpartitions = 3
kafka_key = os.getenv("KAFKA_KEY")
env = os.getenv("env")

subscribed_topics = list()

def checkForErrors(missingArgs):
    if (len(missingArgs) > 0):
        print(*missingArgs, sep="\n")
        exit(1)
    return

def checkRequiredParameter(paramName, missingArgs):
    value = os.getenv(paramName)
    if (value is None):
        missingArgs.append("You must set required parameter "+paramName)
    return value

def convertDate(field):
    retVal=None
    try:
        if (field != None):
            retVal= pd.to_datetime(field)
    except:   # ignore if unable to parse
        logging.exception('')

    return retVal

def getMongoURL(missingArgs):
    database_user = checkRequiredParameter("database_user", missingArgs)
    database_password = checkRequiredParameter("database_password", missingArgs)
    database_name = checkRequiredParameter("database_name", missingArgs)
    database_host = checkRequiredParameter("database_host", missingArgs)
    database_port = checkRequiredParameter("database_port", missingArgs)
    database_replica_name = os.getenv("database_replica_name")
    database_max_pool_size = os.getenv("database_max_pool_size")

    checkForErrors(missingArgs)

    mongoURL = 'mongodb://%s:%s@%s:%s/?' % (database_user, database_password, database_host, database_port)

    if (database_replica_name is not None):
        mongoURL+='replicaSet=%s&' % database_replica_name

    mongoURL+='authSource=%s' % database_name

    if (database_max_pool_size is not None):
        mongoURL+='&maxPoolSize=%s' % database_max_pool_size

    return mongoURL

def cleanApiExplorerRecord(item):
    try:
        item['_id'] = ObjectId(item.pop('id'))
        if ('description' in item and item['description'] == 'unknown'):
            item['description'] = None
        if ('description' in item and item['description'] != None and item['description'] != ""):
            item['descriptionHtml'] = commonmark.commonmark(item['description'])
            item['descriptionText'] = lxml.html.fromstring(item['descriptionHtml']).text_content()
        else:
            item['descriptionHtml'] = None
            item['descriptionText'] = None
        if ('termsOfService' in item and item['termsOfService'] == 'unknown'):
            item['termsOfService'] = None
        if ('certified' in item and item['certified']):
            item['certified']['date'] = convertDate(item['certified']['date'])
        if ('swaggerUrl' in item and item['swaggerUrl'] != None and item['swaggerUrl'].startswith("https://github.optum.com/API-Certification")):
            item['swaggerUrl'] = item['swaggerUrl'].replace("raw/","").replace("https://github.optum.com/API-Certification", "https://github.optum.com/raw/API-Certification")
        if ('apiOwner' in item and item['apiOwner'] != None and item['apiOwner']['email'] == None):
            item['apiOwner']['email'] = ""
        return item
    except Exception as e:
        log.info("Error for the below item:")
        log.info("item:"+str(item))
        log.info(e)
        return None

def cleanApiExplorerExceptionRecord(item):
    try:
        item['_id'] = ObjectId(item.pop('id'))
        item['grant_date'] = convertDate(item['grant_date'])
        if ('description' in item and item['description'] == 'unknown'):
            item['description'] = None

        if ('remediation_date' in item):
            item['remediation_date'] = convertDate(item['remediation_date'])

        if ('remediation_date' in item and item['remediation_date'] == None):
            item.pop('remediation_date')
        for dl in item['dl_list']:
            if dl != None:
                if 'none provided' in dl:
                    item['dl_list'] = []
            elif '''name''' not in dl:
                item['dl_list'] = [{'name': dl}]
        return item
    except Exception as e:
        log.info("Error for the below item:")
        log.info("item:"+str(item))
        log.info(e)
        return None

def updateApiExplorerData(colName, data):
    api_exp_req = list()
    try:
        api_exp_req.append(UpdateOne({'_id': data['_id']}, {'$set': data}, upsert=True))
        results = db[colName].bulk_write(api_exp_req)
    except Exception as e:
            log.info(e)
            results = None
    return results

def removeApiExplorerData(colName, data):
    try:
        results = db[colName].delete_one({"_id" : ObjectId(data['id'])})
    except Exception as e:
        log.info(e)
        results = None
    return results

def processApiDataTopic(badRecords, messageOperation, messageData):
    if (str(messageOperation) == 'a' or str(messageOperation) == 'u'):
        cleanedItem = cleanApiExplorerRecord(messageData)
        if cleanedItem is None:
                badRecords.append(messageData)
        log.info('Number of bad records (will not be updated): ' + str(len(badRecords)))
        results = updateApiExplorerData('api', cleanedItem)
        if results is not None:
            log.info('===== MongoDB upsert results ====={}'.format(results.bulk_api_result))
    elif (str(messageOperation) == 'r'):
        results = removeApiExplorerData('api', messageData)
        if results is not None:
                log.info('Number of deleted documents: {}'.format(results.deleted_count))

def processApiExceptionTopic(badRecords, messageOperation, messageData):
    if (str(messageOperation) == 'a' or str(messageOperation) == 'u'):
        cleanedItem = cleanApiExplorerExceptionRecord(messageData)
        if cleanedItem is None:
                badRecords.append(messageData)
        log.info('Number of bad records (will not be updated): ' + str(len(badRecords)))
        results = updateApiExplorerData('modelApiException', cleanedItem)
        if results is not None:
            log.info('===== MongoDB upsert results ====={}'.format(results.bulk_api_result))
    elif (str(messageOperation) == 'r'):
        results = removeApiExplorerData('modelApiException', messageData)
        if results is not None:
                log.info('Number of deleted documents: {}'.format(results.deleted_count))

missingArgs=[]
apiDataTopic = checkRequiredParameter("apiDataTopic",missingArgs)
apiExceptionTopic = checkRequiredParameter("apiExceptionTopic",missingArgs)
BOOTSTRAP = checkRequiredParameter("BOOTSTRAP",missingArgs)
AVRO_REPO = checkRequiredParameter("AVRO_REPO",missingArgs)
OFFSET = checkRequiredParameter("OFFSET",missingArgs)
groupid = checkRequiredParameter("groupid",missingArgs)

checkForErrors(missingArgs)

log.info("This is product-catalog-kafka-consumer start")

mongoURL = getMongoURL(missingArgs)
mongoclient = MongoClient(mongoURL)
db = mongoclient.sampledb

if (env == "prod"):
    config = {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": groupid,
                "security.protocol": 'SSL',
                "schema.registry.url": AVRO_REPO,
                "ssl.ca.location": 'certs/prod/caroot.pem',
                "ssl.certificate.location": 'certs/prod/certificate.pem',
                "ssl.key.location": kafka_key
    }
else:
    config = {
                "bootstrap.servers": BOOTSTRAP,
                "group.id": groupid,
                "security.protocol": 'SSL',
                "schema.registry.url": AVRO_REPO,
                "ssl.ca.location": 'certs/non-prod/caroot.pem',
                "ssl.certificate.location": 'certs/non-prod/certificate.pem',
                "ssl.key.location": kafka_key
    }

# no local avro schema setup when we use repository
consumer = AvroConsumer(config)

if (env == "local"):
    topicPartitionData=[TopicPartition(apiDataTopic, p) for p in range(0,mytopicpartitions)]
    topicPartitionException=[TopicPartition(apiExceptionTopic, p) for p in range(0,mytopicpartitions)]
    subscribed_topics.extend(topicPartitionException)
    subscribed_topics.extend(topicPartitionData)
    consumer.assign(subscribed_topics)
else:
    subscribed_topics.append(apiDataTopic)
    subscribed_topics.append(apiExceptionTopic)
    consumer.subscribe(subscribed_topics)


log.info("Consumer is listening for messages coming to topics: "+str(subscribed_topics))
badRecords = list()
# Read all messages from 2 topics
while True:
     try:
         msg = consumer.poll(2)
         badRecords.clear()
         if msg is None:
            continue
         if msg.error():
            log.error("Consumer error: {}".format(msg.error()))
            continue
         if len(msg):
             log.info("topic:"+str(msg.topic()))
             message = msg.value()
             messageOperation = message['op'][0]
             log.info("Operation: "+str(messageOperation))
             messageData = message['data']
             if (str(msg.topic()) == apiDataTopic):
                 processApiDataTopic(badRecords, messageOperation, messageData)
             elif (str(msg.topic()) == apiExceptionTopic):
                 processApiExceptionTopic(badRecords, messageOperation, messageData)
         log.info("committed:"+str(consumer.committed(subscribed_topics)))
         consumer.commit()
     except Exception as e:
         log.error(str(e))
         continue
consumer.close()
