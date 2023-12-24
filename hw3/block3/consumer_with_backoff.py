from kafka import KafkaConsumer
import time
import json


def backoff(tries=10, sleep=2):
    def inner(func):
        def wrapper(*args, **kwargs):
            for _ in range(tries):
                try:
                    func(*args, **kwargs)
                except:
                    time.sleep(sleep)
                else:
                    break
            else:
                raise RuntimeError("Cannot run message handler")
        return wrapper
    return inner
    
    
@backoff(tries=3,sleep=2)
def message_handler(value)->None:
    data = json.loads(value.value.decode("utf-8"))
    t = 1/(data['device_id']%2) # error emulation
    print(value)

# @backoff(tries=3,sleep=2)
# def message_handler(value)->None:
#     print(value)


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer("itmo2023",
                             group_id='itmo_group1',
                             bootstrap_servers='localhost:29092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    for message in consumer:
        message_handler(message)
        print(message)


if __name__ == '__main__':
    create_consumer()
