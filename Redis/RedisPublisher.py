# - read from kafka
# - write to redis message queue
from kafka import KafkaConsumer
import argparse
import atexit
import redis
import logging

topic_name = 'average-stock-analyzer'
kafka_broker = '192.168.99.100:9092'
redis_channel = 'redis-stock-analyzer'
redis_host = '192.168.99.100'
redis_port = '6379'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('redis-publisher')
logger.setLevel(logging.DEBUG)

def shutdown_hook():
    logger.info('shutdown kafka consumer')
    try:
        kafka_consumer.close()
    except Exception as e:
        logger.info('Failed to close kafka consumer')

if __name__ == '__main__':
    # - setup arguments
    # parser = argparse.ArgumentParser()
    # parser.add_argument('topic_name', help='the kafka topic to read from')
    # parser.add_argument('kafka_broker', help='the location of kafka broker')
    # parser.add_argument('redis_channel', help='the redis channel to publish to')
    # parser.add_argument('redis_host', help='redis server ip')
    # parser.add_argument('redis_port', help='redis server port')
    #
    # args = parser.parse_args()
    # topic_name = args.topic_name
    # kafka_broker = args.kafka_broker
    # redis_channel = args.redis_channel
    # redis_host = args.redis_host
    # redis_port = args.redis_port

    # - setup kafka consumer
    kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

    # - setup redis client
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

    atexit.register(shutdown_hook)
    # - write message to redis message queue
    for msg in kafka_consumer:
        logger.info('Received message from kafka %s' %str(msg))
        redis_client.publish(redis_channel, msg.value)


