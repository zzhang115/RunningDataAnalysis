# - read from kafka
# - write to Redis message queue
from Kafka import KafkaConsumer
import argparse
import atexit
import Redis
import logging

topic_name = 'average-running-analyzer'
kafka_broker = '192.168.99.100:9092'
redis_channel = 'Redis-running-analyzer'
redis_host = '192.168.99.100'
redis_port = '6379'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('Redis-publisher')
logger.setLevel(logging.DEBUG)

def shutdown_hook():
    logger.info('shutdown Kafka consumer')
    try:
        kafka_consumer.close()
    except Exception as e:
        logger.info('Failed to close Kafka consumer')

if __name__ == '__main__':
    # - setup arguments
    # parser = argparse.ArgumentParser()
    # parser.add_argument('topic_name', help='the Kafka topic to read from')
    # parser.add_argument('kafka_broker', help='the location of Kafka broker')
    # parser.add_argument('redis_channel', help='the Redis channel to publish to')
    # parser.add_argument('redis_host', help='Redis server ip')
    # parser.add_argument('redis_port', help='Redis server port')
    #
    # args = parser.parse_args()
    # topic_name = args.topic_name
    # kafka_broker = args.kafka_broker
    # redis_channel = args.redis_channel
    # redis_host = args.redis_host
    # redis_port = args.redis_port

    # - setup Kafka consumer
    kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

    # - setup Redis client
    redis_client = Redis.StrictRedis(host=redis_host, port=redis_port)

    atexit.register(shutdown_hook)
    # - write message to Redis message queue
    for msg in kafka_consumer:
        logger.info('Received message from Kafka %s' %str(msg))
        redis_client.publish(redis_channel, msg.value)


