# - read from kafka
# - write to redis message queue
from kafka import KafkaConsumer
import argparse
import atexit
import logging
import redis
