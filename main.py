import sys
import socket
import argparse
import stdiocolours
from enum import Enum
from confluent_kafka import Producer
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

import time
import logging

def main():
    args = get_args()
    producer = Producer({
        'bootstrap.servers': args.brokers,
        'client.id': socket.gethostname()
    })
    if args.operation is Operation.PRODUCE:
        for topic in args.topics.split(','):
            producer.produce(topic=topic.strip(), value=args.message, callback=lambda err, msg: message_ack(err, msg, topic))
            producer.poll(5)
    if args.operation is Operation.LIST_TOPICS:
        print(stdiocolours.OKBLUE + "\nTOPICS:" + stdiocolours.ENDC)
        count = 1
        for topic in producer.list_topics().topics:
            print(str(count) + ":", topic)
            count += 1
    if args.operation is Operation.WATCH_PRODUCE:
        watch_dir()


def message_ack(err, msg, topic):
    if err is not None:
        print(stdiocolours.FAIL + "\nTOPIC = %s \nFailed to deliver message: %s: %s" % (topic, str(msg), str(err)) + stdiocolours.ENDC, "\n")
    else:
        print(stdiocolours.OKGREEN + "\nTOPIC = %s \nMessage sent: %s" % (topic, str(msg)) + stdiocolours.ENDC, "\n")

class Operation(Enum):
    PRODUCE = 'produce'
    WATCH_PRODUCE = 'watch-produce'
    CONSUME = 'consume'
    LIST_TOPICS = 'list-topics'


def watch_dir():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    event_handler = LoggingEventHandler()
    observer = Observer()
    observer.schedule(event_handler, '.', recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

def get_args():
    parser = argparse.ArgumentParser(description='A helper to send data to a Kafka cluster')
    parser.add_argument('--operation', type=Operation, default='', required=False,
                        help='The operation to carry out')
    parser.add_argument('--brokers', type=str, default='', required=False,
                        help='A comma-separated list of broker addresses')
    parser.add_argument('--message', type=str, default='',
                        help='A single message to send to Kafka')
    parser.add_argument('--topics', type=str, default='',
                        help='A comma-separated list of topics to produce or subscribe to')
    return parser.parse_args()    

if __name__ == "__main__":
    main()