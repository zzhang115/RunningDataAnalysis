# specify kafka cluster and its topic to send event
# specify one share and scrapy share information per seconds

import argparse

if __name__ == '__main__':
    # setup commandline arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help = 'symbol of stock')
    parser.add_argument('topic_name', help = 'kafka topic')
    parser.add_argument('kafka_broker', help = 'the location of kafka broker')

    #


