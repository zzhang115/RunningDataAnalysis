# specify kafka cluster and its topic to send event
# specify one share and scrapy share information per seconds

from Kafka import KafkaProducer
from googlefinance import getQuotes
from Kafka.errors import KafkaError, KafkaTimeoutError
from flask import Flask, request, jsonify

import argparse
import json
import time
import logging
import schedule
import atexit # it means when it exit, this is charge to do something

# default Kafka setting
# topic_name = 'running-analysis'
# kafka_broker = '127.0.0.1:9092'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
# set logger level: TRACE, INFO(give you some information), DEBUG, WARNING, ERROR
logger.setLevel(logging.DEBUG)

def fetch_data(producer, symbol):
    #@param producer - instance of a Kafka producer
    #@param symbol - symbol of the running, string type
    #@return None
    logger.debug('Start to fetch running data for %s', symbol)
    try:
        data = json.dumps(getQuotes(symbol))#.encode('utf-8')
        logger.debug('Get running data %s', data)
        # producer.send(topic = topic_name, value = data, timestamp_ms = time.time())
        # print(time.strftime('%Y-%m-%d %H:%M:%S'))
        # value format can affect sending process, it may leads fail
        # the following function is used to send json message to Kafka
        producer.send(topic='running-analyzer', value=data)
        # the following function is just to send string message to Kafka
        # producer.send(topic='running-analyzer', value=b'running-analyzer', timestamp_ms=None)
        logger.debug('Sent running data for %s to Kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send running data for %s to Kafka, caused by %s', (symbol, timeout_error))
    except Exception:
        logger.warn('Failed to send running data for %s', symbol)

def shutdown_hook(producer):
    try:
        producer.flush(10)
    except KafkaError as kafkaError:
        logger.warn('Failed to flush pending messages to Kafka')
    finally:
        try:
            producer.close()
            logger.info('Kafka connection has been closed')
        except Exception as e:
            logger.warn('Failed to close Kafka connection')

def add_running(symbol):
    symbols = set()
    if not symbol:
        return jsonify({
            'error': 'running symbol cannot be empty'
        }), 400
    if symbol in symbols:
        pass
    else:
        symbol = symbol.encode('utf-8')
        symbols.add(symbol)
        logger.info('Add running retrieve job %s' % symbol)
        schedule.add_job(fetch_data, 'interval', [symbol], seconds=1, id=symbol)
    return jsonify(results=list(symbols)), 200

def del_running(symbol):
    symbols = set()
    logger.info('remove the %s' %symbol)
    if not symbol:
        return jsonify({
            'error': 'running symbol cannot be empty'
        }), 400
    if symbol not in symbols:
        pass
    else:
        symbol = symbol.encode('utf-8')
        logger.info('remove the %s' %symbol)
        symbols.remove(symbol)
        schedule.remove_job(symbol)
    return jsonify(results=list(symbols)), 200

if __name__ == '__main__':
    # setup commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help = 'symbol of running')
    parser.add_argument('topic_name', help = 'Kafka topic')
    parser.add_argument('kafka_broker', help = 'the location of Kafka broker')

    # parse arguments
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    symbol = 'AAPL'
    topic_name = 'running-analyzer'
    kafka_broker = '192.168.99.100:9092'

    # instantiate a Kafka producer
    # producer = KafkaProducer(bootstrap_servers=kafka_broker)
    producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    fetch_data(producer, symbol)

    # schedule to run every 1 second
    schedule.every(2).seconds.do(fetch_data, producer, symbol)
    # setup proper shutdown hook
    atexit.register(shutdown_hook, producer) # before it exit, it will call this function to close Kafka connection
    # its exit case includes if we interrupt this python program by keyboard or before it ends normally

    while True:
        schedule.run_pending()
        time.sleep(1)

# 1. write log info is very important
# 2. atexit, we write a shutdown_hook program is very necessary
# 3. Kafka it depends on zookeeper, before we start Kafka, we need to start zookeeper firstly
