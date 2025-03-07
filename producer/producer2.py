from kafka import KafkaProducer
import json
from data import get_customer_user
import time

def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def get_partition(key,all,available):
    return 1



producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)


if __name__ == "__main__":

    while 1==1:
        customer_data = get_customer_user()
        print("before")
        print(customer_data)
        print("after")
        producer.send("third_topic" ,customer_data)
        time.sleep(4)

