from kafka import KafkaProducer
from kafka import errors as Errors
import time

'''
    Kafka Producer used in class
'''

__author__ = 'Manuel'


def main():
    producer = KafkaProducer(bootstrap_servers=['localhost:9094'])

    future=producer.send('masterbigdata', b'Hello Class!') # Send the message
    #future=producer.send('masterbigdata',key=b"temp", value=b'temperature-value')
    
    producer.flush()  # Wait until all pending messages are at least put on the network

    producer.close()  # Close the connection


if __name__ == "__main__":
    main()
