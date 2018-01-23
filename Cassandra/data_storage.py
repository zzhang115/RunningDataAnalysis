from Cassandra.cluster import Cluster
from Kafka import KafkaConsumer
from Kafka.errors import KafkaError, KafkaTimeoutError

import argparse
import json
import time
import logging
import schedule
import atexit # it means when it exit, this is charge to do something
from ast import literal_eval

# default Kafka and Cassandra setting
topic_name = 'stock-analyzer'
kafka_broker = '192.168.99.100:9092'
keyspace = 'stock'
table = 'stock'
cassandra_broker = ['192.168.99.100']

# - logging file configuration
def logger(self):
    Format="%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(format=Format)
    logger=logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger

def create(self):
    self.cassandra_session.execute(
        "create keyspace if not exists %s with replication={'class':'SimpleStrategy', "
        "'replication_factor':'3'} and durable_writes= 'true'" % self.__keyspace)
    self.cassandra_session.set_keyspace(self.__keyspace)
    self.cassandra_session.execute(
        "create table if not exists %s(stock_symbol text,trade_time timestamp,trade_price float, "
        "primary key ((stock_symbol),trade_time))" % self.__data_table)

def persist_data(stock_data, cassandra_session):
    '''
    :param stock_data:
    :param cassandra_session: a session created using Cassandra-driver
    :return: None
    '''
    logger.debug('Start to persist data to Cassandra%s', stock_data)
    # - stock_data we get is just byte array, we need to transfer it to json array string, then use literal_eval to transfter string to list
    stock_data = literal_eval(stock_data.decode('utf-8'))
    json_dict= json.loads(stock_data)[0]
    # decode_stock_data = stock_data.decode('utf-8').replace('\\\"', '\"').replace('\"[', '').replace(']\"', '')
    symbol = json_dict.get('StockSymbol')
    price = float(json_dict.get('LastTradePrice'))
    tradetime = json_dict.get('LastTradeDateTime')
    statement = 'INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES(\'%s\', \'%s\', %f)' %(table, symbol, tradetime, price)
    cassandra_session.execute(statement)
    logger.info('finish insert data into Cassandra table')

def shutdown_hook(consumer, session):
    consumer.close()
    logger.info('Kafka consumer has been closed')
    session.shutdown()
    logger.info('Cassandra session has been closed')

if __name__ == '__main__':
    # - set commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the Kafka topic')
    parser.add_argument('kafka_broker', help='the location of Kafka broker')
    parser.add_argument('keyspace', help='the keyspace to be used in Cassandra')
    parser.add_argument('data_table', help='the data table to be used in Cassandra')
    parser.add_argument('cassandra_broker', help='the location of Cassandra broker')

    # - parse arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    keyspace = args.keyspace
    data_table = args.data_table
    cassandra_broker = args.cassandra_broker

    # - setup a Kafka consumer
    consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

    # - setup a Cassandra
    cassandra_cluster = Cluster(contact_points=cassandra_broker)
    session = cassandra_cluster.connect(keyspace)
    atexit.register(shutdown_hook, consumer, session)

    # - implement a function to persist data to Cassandra
    for msg in consumer:
        persist_data(msg.value, session)
