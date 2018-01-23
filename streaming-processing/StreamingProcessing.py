# 1, read from kafka, kafka broker, kafka topic
# 2, write data back to kafka, kafka broker, kafka topic
# under the current path and run spark-submit --jars *.jar StreamingProcessing.py

import sys
import atexit
import logging
import json
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from ast import literal_eval

topic = 'stock-analyzer'
new_topic = 'average-stock-analyzer'
kafka_broker = '192.168.99.100:9092'
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-process')
logger.setLevel(logging.INFO)

def process_stream(stream):
    def send_to_kafka(rdd):
        results = rdd.collect()
        for r in results:
            data = json.dumps(
                {
                    'symbol' : r[0],
                    'timestamp' : time.time(),
                    'average' : int(r[1][0]) / int(r[1][1])
                }
            )
            print('data:', data.encode('utf-8'))
            try:
                logger.info('Sending average price %s to kafka' %data)
                kafka_producer.send(new_topic, value = data.encode('utf-8'))
            except KafkaError as error:
                logger.warn('Failed to send average stock price to kafka, casued by %s', error.message)

    def pair(data):
        record = json.loads(literal_eval(data[1]))[0]
        print(record.get('StockSymbol'), '---', (float(record.get('LastTradePrice')), 1))
        return record.get('StockSymbol'), (float(record.get('LastTradePrice')), 1)

    # def pair2(symbol, x_y): # missing 1 required positional argument: 'x_y'
    #     return symbol, x_y[0] / x_y[1]
    # stream receive not just one message at the same time, so it use reduceByKey
    stream.map(pair).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).foreachRDD(send_to_kafka)

    # num_of_records = rdd.count()
    # print(num_of_records)
    # if num_of_records == 0:
    #     return
    # print(rdd.map(lambda record : record))
    # price_sum = rdd.map(lambda record: float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePrice'))).reduce(lambda  a, b : a + b)
    # average_price = price_sum / num_of_records
    # stock_data = literal_eval(stock_data.decode('utf-8'))
    # json_dict= json.loads(stock_data)[0]
    # price = float(json_dict.get('LastTradePrice'))
    # parsed = stream.map(lambda v : json.loads(v[1]))
    # lines = stream.map(lambda x : x[1])
    # print(lines)

def shutdown_hook(producer):
    try:
        logger.info('Flush pending messages to kafka')
        # - flush(10) 10 is ten seconds timeout
        producer.flush(10)
        logger.info('Finish flushing pending message')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending message to kafka')
    finally:
        try:
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection')
        logger.info('Finish closing kafka producer')

if __name__ == '__main__':
    # if(len(sys.argv) != 4):
    #     print('Usage: streaming processing [topic] [new_topic] [kafka_broker]')
    #     exit(1)
    # topic, new_topic, kafka_borker = sys.argv[1:]
    # - setup connection to spark cluster
    # - 2 means how many cores we use for computation
    # - spark program name
    sc = SparkContext('local[2]', 'StockAveragePrice')
    # - spark has its own logger
    sc.setLogLevel('ERROR')
    # - similar to water tap, open water tap per 5 seconds to handle data
    ssc = StreamingContext(sc, 5)
    # - create a data stream from spark
    # directKafkaStream = KafkaUtils.createStream(ssc, kafka_borker, 'spark-streaming-consumer',{topic : 1})
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list' : kafka_broker})
    # - for each RDD, do something
    process_stream(directKafkaStream)
    # - instantiate kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)
    # - setup proper shutdown hook
    atexit.register(shutdown_hook, kafka_producer)
    ssc.start()
    ssc.awaitTermination()

