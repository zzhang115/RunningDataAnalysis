# specify kafka cluster and its topic to send event
# specify one share and scrapy share information per seconds

from kafka import KafkaProducer
from googlefinance import getQuotes
import argparse
import json
# default kafka setting
topic_name = 'stock_analysis'
kafka_broker = '127.0.0.1:9002'

def fetch_price(producer, symbol):
    #@param producer - instance of a kafka producer
    #@param symbol - symbol of the stock, string type
    #@return None
    price = json.dumps(getQuotes(symbol))


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

    # initiate a kafka producer
    producer = KafkaProducer(bootstrap_servers = kafka_broker)




