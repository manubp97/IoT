from kafka import KafkaConsumer

'''
    Kafka Consumer used in class.
'''

__author__ = 'Manuel'


def main():
    consumer = KafkaConsumer('masterbigdata', 
    	bootstrap_servers=['localhost:9094'])
    try:
        for msg in consumer:
        	print(msg)  # print the total msg
        	print(msg.value.decode("utf-8"))  # print the msg value
    except KeyboardInterrupt:
        consumer.close()
        print("Bye!")



if __name__ == "__main__":
    main()
