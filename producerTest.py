import time

from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError


def print_response(response=None):
    if response:
        print('Error: {0}'.format(response[0].error))
        print('Offset: {0}'.format(response[0].offset))


def main():
    kafka = KafkaClient("192.168.99.100:9092")
    producer = SimpleProducer(kafka)

    topic = 'stock-analyzer'
    msg = b'Hello World'
    try:
        print_response(producer.send_messages(topic, msg))
    except LeaderNotAvailableError:
        # https://github.com/mumrah/kafka-python/issues/249
        time.sleep(1)
        print_response(producer.send_messages(topic, msg))

    kafka.close()


if __name__ == "__main__":
    main()