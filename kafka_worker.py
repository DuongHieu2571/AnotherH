
from settings.config import Setting
from worker.consumer import Consumer



if __name__=="__main__":
    consumer = Consumer(host=Setting.KAFKA_HOST, port=Setting.KAFKA_PORT, topic=Setting.KAFKA_TOPIC)
    consumer()