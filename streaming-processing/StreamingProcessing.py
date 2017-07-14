# 1, read from kafka, kafka broker, kafka topic
# 2, write data back to kafka, kafka broker, kafka topic

import sys

topic = ''
new_topic = ''
kafka_borker = ''
if __name__ == '__main__':
    if(len(sys.argv) != 4):
        print('Usage: streaming processing [topic] [new_topic] [kafka_broker')
        exit(1)



