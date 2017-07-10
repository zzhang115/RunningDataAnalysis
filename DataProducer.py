# specify kafka cluster and its topic to send event
# specify one share and scrapy share information per seconds

from kafka import KafkaProducer
from googlefinance import getQuotes
from kafka.errors import KafkaError, KafkaTimeoutError

import argparse
import json
import time
import logging
import schedule

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
        price = json.dumps(getQuotes(symbol))
        logger.debug('Get stock price %s', price)
        producer.send(topic = topic_name, value = price, timestamp_ms = time.time())
        logger.debug('Sent stock price for %s to kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send stock price for %s to kafka, caused by %s', (symbol, timeout_error))
    except Exception:
        logger.warn('Failed to fetch stock price for %s', symbol)

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

    # instantiate a kafka producer
    producer = KafkaProducer(bootstrap_servers = kafka_broker)
    fetch_price(producer, symbol)

    # schedule to run every 1 second
    # schedule.every(1).second.do(fetch_price, producer, symbol)
    #
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)




