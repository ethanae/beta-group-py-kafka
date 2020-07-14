import socket
import argparse
from confluent_kafka import Producer
from enum import Enum

def main():
    args = get_args()
    print(args)
    producer = Producer({
        'bootstrap.servers': args.brokers,
        'client.id': socket.gethostname()
    })
    if (args.operation is Operation.SEND_MESSAGE):
        print('producer.produce()')


class Operation(Enum):
    SEND_MESSAGE = 'send-message'

def get_args():
    parser = argparse.ArgumentParser(description='A helper to send data to a Kafka cluster')
    parser.add_argument('--operation', type=Operation, default='', required=True,
                        help='The operation to carry out')
    parser.add_argument('--brokers', type=str, default='', required=True,
                        help='A comma-separated list of broker addresses')
    return parser.parse_args()    

if __name__ == "__main__":
    main()