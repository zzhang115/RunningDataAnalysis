# - need to read from kafka, topic
# - need to write to cassandra, table

from kafka import KafkaProducer
from googlefinance import getQuotes
from kafka.errors import KafkaError, KafkaTimeoutError

import argparse
import json
import time
import logging
import schedule
import atexit # it means when it exit, this is charge to do something

# default kafka and cassandra setting
topic_name = 'stock-analyzer'
kafka_broker = '192.168.99.100:9092'
keyspace = 'stock'
data_table = ''
cassandra_broker = '192.168.99.100:9042'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage')
# set logger level: TRACE, INFO(give you some information), DEBUG, WARNING, ERROR
logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the kafka topic')
    parser.add_argument('kafka_broker', help='the location of kafka broker')
    parser.add_argument('keyspace', help='the keyspace to be used in cassandra')
    parser.add_argument('data_table', help='the data table to be used in cassandra')
    parser.add_argument('cassandra_broker', help='the location of cassandra broker')

    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    keyspace = args.keyspace
    data_table = args.data_table
    cassandra_broker = args.cassandra_broker
