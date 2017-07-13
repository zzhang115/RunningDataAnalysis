# - need to read from kafka, topic
# - need to write to cassandra, table

from kafka import KafkaConsumer
from cassandra.cluster import Cluster
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
data_table = 'stock'
cassandra_broker = ['192.168.99.100']

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage')
# set logger level: TRACE, INFO(give you some information), DEBUG, WARNING, ERROR
logger.setLevel(logging.DEBUG)

def persist_data(stock_data, cassandra_session):
    '''
    :param stock_data:
    :param cassandra_session: a session created using cassandra-driver
    :return: None
    '''
    # logger.debug('Start to persist data to cassandra%s', stock_data)
    parser = json.loads(stock_data)[0]
    symbol = parser.get('StockSymbol')
    price = float(parser.get('LastTradePrice'))
    tradetime = parser.get('LastTradeDateTime')
    print(price, '---', tradetime)
    statement = 'INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES(%s, %s, %f)' %(symbol, tradetime, price)
    logger.info()

def shutdown_hook(consumer, session):
    consumer.close()
    logger.info('Kafka consumer has been closed')
    session.close()
    logger.info('Cassandra session has been closed')

if __name__ == '__main__':
    # - set commandline arguments
    # parser = argparse.ArgumentParser()
    # parser.add_argument('topic_name', help='the kafka topic')
    # parser.add_argument('kafka_broker', help='the location of kafka broker')
    # parser.add_argument('keyspace', help='the keyspace to be used in cassandra')
    # parser.add_argument('data_table', help='the data table to be used in cassandra')
    # parser.add_argument('cassandra_broker', help='the location of cassandra broker')

    # - parse arguments
    # args = parser.parse_args()
    # topic_name = args.topic_name
    # kafka_broker = args.kafka_broker
    # keyspace = args.keyspace
    # data_table = args.data_table
    # cassandra_broker = args.cassandra_broker

    # - setup a kafka consumer
    consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

    # - setup a cassandra
    cassandra_cluster = Cluster(contact_points=cassandra_broker)
    session = cassandra_cluster.connect(keyspace)
    for msg in consumer:
        # - implement a function to persist data to cassandra
        persist_data(msg.value, session)
