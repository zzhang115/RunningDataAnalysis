# specify kafka cluster and its topic to send event
# specify one share and scrapy share information per seconds

from kafka import KafkaProducer
from googlefinance import getQuotes
from kafka.errors import KafkaError, KafkaTimeoutError
from flask import Flask, request, jsonify

import argparse
import json
import time
import logging
import schedule
import atexit # it means when it exit, this is charge to do something


# default kafka setting
# topic_name = 'stock-analysis'
# kafka_broker = '127.0.0.1:9092'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
# set logger level: TRACE, INFO(give you some information), DEBUG, WARNING, ERROR
logger.setLevel(logging.DEBUG)

def fetch_price(producer, symbol):
    #@param producer - instance of a kafka producer
    #@param symbol - symbol of the stock, string type
    #@return None
    logger.debug('Start to fetch stock price for %s', symbol)
    try:
        price = json.dumps(getQuotes(symbol))#.encode('utf-8')
        logger.debug('Get stock price %s', price)
        # producer.send(topic = topic_name, value = price, timestamp_ms = time.time())
        # print(time.strftime('%Y-%m-%d %H:%M:%S'))
        # value format can affect sending process, it may leads fail
        # the following function is used to send json message to kafka
        producer.send(topic='stock-analyzer', value=price)
        # the following function is just to send string message to kafka
        # producer.send(topic='stock-analyzer', value=b'stock-analyzer', timestamp_ms=None)
        logger.debug('Sent stock price for %s to kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send stock price for %s to kafka, caused by %s', (symbol, timeout_error))
    except Exception:
        logger.warn('Failed to send stock price for %s', symbol)

def shutdown_hook(producer):
    try:
        producer.flush(10)
    except KafkaError as kafkaError:
        logger.warn('Failed to flush pending messages to kafka')
    finally:
        try:
            producer.close()
            logger.info('kafka connection has been closed')
        except Exception as e:
            logger.warn('Failed to close kafka connection')

def add_stock(symbol):
    symbols = set()
    if not symbol:
        return jsonify({
            'error': 'Stock symbol cannot be empty'
        }), 400
    if symbol in symbols:
        pass
    else:
        symbol = symbol.encode('utf-8')
        symbols.add(symbol)
        logger.info('Add stock retrieve job %s' % symbol)
        schedule.add_job(fetch_price, 'interval', [symbol], seconds=1, id=symbol)
    return jsonify(results=list(symbols)), 200

def del_stock(symbol):
    symbols = set()
    logger.info('remove the %s' %symbol)
    if not symbol:
        return jsonify({
            'error': 'Stock symbol cannot be empty'
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
    parser.add_argument('symbol', help = 'symbol of stock')
    parser.add_argument('topic_name', help = 'kafka topic')
    parser.add_argument('kafka_broker', help = 'the location of kafka broker')

    # parse arguments
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    symbol = 'AAPL'
    topic_name = 'stock-analyzer'
    kafka_broker = '192.168.99.100:9092'

    # instantiate a kafka producer
    # producer = KafkaProducer(bootstrap_servers=kafka_broker)
    producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    fetch_price(producer, symbol)

    # schedule to run every 1 second
    schedule.every(2).seconds.do(fetch_price, producer, symbol)
    # setup proper shutdown hook
    atexit.register(shutdown_hook, producer) # before it exit, it will call this function to close kafka connection
    # its exit case includes if we interrupt this python program by keyboard or before it ends normally

    while True:
        schedule.run_pending()
        time.sleep(1)

# 1. write log info is very important
# 2. atexit, we write a shutdown_hook program is very necessary
# 3. kafka it depends on zookeeper, before we start kafka, we need to start zookeeper firstly
