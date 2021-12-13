from kafka import KafkaConsumer,TopicPartition

'''
    Kafka Consumer with seeking used in class.
'''

__author__ = 'Manuel'

def main():
    topic='masterbigdata'
    partition=0
    offset=5 # Need more than 5 messages

    consumer = KafkaConsumer(bootstrap_servers=['localhost:9094'])
    topicpartition=TopicPartition(topic, partition) 
    consumer.assign([topicpartition])# Assign the partition
    consumer.seek(topicpartition,offset) # Seek the offset

    print ("Messages:\n")
    try:
        for msg in consumer:
        	print(msg.value.decode("utf-8"))  # print the msg value
    except KeyboardInterrupt:
        consumer.close()
        print("Bye!")

if __name__ == "__main__": 
    main()   
