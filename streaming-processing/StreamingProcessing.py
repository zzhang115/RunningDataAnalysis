# 1, read from kafka, kafka broker, kafka topic
# 2, write data back to kafka, kafka broker, kafka topic

import sys
import atexit
import logging
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

topic = ''
new_topic = ''
kafka_borker = ''
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-process')
logger.setLevel(logging.INFO)

def process(timeobj, rdd):
    print(rdd)
    num_of_records = rdd.count()
    if num_of_records == 0:
        return
    price_sum = rdd.map(lambda record: float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePrice'))).reduce(lambda  a, b : a + b)
    average_price = price_sum / num_of_records
    logger.info('Receive %d records from Kafka, average price is %d', num_of_records, average_price)
    # stock_data = literal_eval(stock_data.decode('utf-8'))
    # json_dict= json.loads(stock_data)[0]
    # price = float(json_dict.get('LastTradePrice'))


def shutdown_hook(producer):
    try:
        logger.info('flush pending messages to kafka')
        # - flush(10) 10 is ten seconds timeout
        producer.flush(10)
        logger.info('finish flushing pending message')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending message to kafka')
    finally:
        try:
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection')

if __name__ == '__main__':
    if(len(sys.argv) != 4):
        print('Usage: streaming processing [topic] [new_topic] [kafka_broker')
        exit(1)
    topic, new_topic, kafka_borker = sys.argv[1:]
    # - setup connection to spark cluster
    # - 2 means how many cores we use for computation
    # - spark program name
    sc = SparkContext('local[2]', 'StockAveragePrice')
    # - spark has its own logger
    sc.setLogLevel('ERROR')
    # - similar to water tap, open water tap per 5 seconds to handle data
    ssc = StreamingContext(sc, 5)
    # - create a data stream from spark
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic],{'metadata.broker.list' : kafka_borker})
    # - for each RDD, do something
    directKafkaStream.foreachRDD(process)
    # - instantiate kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_borker)
    # - setup proper shutdown hook
    atexit.register(shutdown_hook, kafka_producer)
    ssc.start()
    ssc.awaitTermination()

