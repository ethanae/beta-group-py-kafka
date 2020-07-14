import socket
import argparse
from enum import Enum
from confluent_kafka import Producer

def main():
    args = get_args()
    producer = Producer({
        'bootstrap.servers': args.brokers,
        'client.id': socket.gethostname()
    })
    if (args.operation is Operation.SEND_MESSAGE):
        producer.produce(topic=args.topic, value=args.message, callback=message_ack)
        producer.poll(1)

def message_ack(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

class Operation(Enum):
    SEND_MESSAGE = 'send-message'

def get_args():
    parser = argparse.ArgumentParser(description='A helper to send data to a Kafka cluster')
    parser.add_argument('--operation', type=Operation, default='', required=True,
                        help='The operation to carry out')
    parser.add_argument('--brokers', type=str, default='', required=True,
                        help='A comma-separated list of broker addresses')
    parser.add_argument('--message', type=str, default='',
                        help='A JSON string of the message to send')
    parser.add_argument('--topic', type=str, default='',
                        help='The topic to which to send the message')
    return parser.parse_args()    

if __name__ == "__main__":
    main()