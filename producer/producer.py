from kafka import KafkaProducer
import json
from data import get_registered_user
import time

def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def get_partition(key,all,available):
    return 1



producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)


if __name__ == "__main__":

    while 1==1:
        registered_user = get_registered_user()
        print("before")
        print(registered_user)
        print("after")
        producer.send("second_topic" ,registered_user)
        time.sleep(4)
