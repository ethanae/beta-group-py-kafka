import socket
import argparse
import stdiocolours
from enum import Enum
from confluent_kafka import Producer

def main():
    args = get_args()
    print(args.topics.split(','))
    # exit(0)
    producer = Producer({
        'bootstrap.servers': args.brokers,
        'client.id': socket.gethostname()
    })
    if (args.operation is Operation.PRODUCE):
        for topic in args.topics.split(','):
            producer.produce(topic=topic, value=args.message, callback=lambda err, msg: message_ack(err, msg, topic))
            producer.poll(5)

def message_ack(err, msg, topic):
    if err is not None:
        print(stdiocolours.FAIL + "\nTOPIC = %s \nFailed to deliver message: %s: %s" % (topic, str(msg), str(err)) + stdiocolours.ENDC, "\n")
    else:
        print(stdiocolours.OKGREEN + "\nTOPIC = %s \nMessage sent: %s" % (topic, str(msg)) + stdiocolours.ENDC, "\n")

class Operation(Enum):
    PRODUCE = 'produce'
    CONSUME = 'consume'

def get_args():
    parser = argparse.ArgumentParser(description='A helper to send data to a Kafka cluster')
    parser.add_argument('--operation', type=Operation, default='', required=True,
                        help='The operation to carry out')
    parser.add_argument('--brokers', type=str, default='', required=True,
                        help='A comma-separated list of broker addresses')
    parser.add_argument('--message', type=str, default='',
                        help='A single message to send to Kafka')
    parser.add_argument('--topics', type=str, default='',
                        help='A comma-separated list of topics to produce or subscribe to')
    return parser.parse_args()    

if __name__ == "__main__":
    main()