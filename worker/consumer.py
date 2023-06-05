from kafka import KafkaConsumer
import json
from helpers.logging import Logger


class Consumer():
    def __init__(self, host, port, topic) -> None:
        # init consumer instane
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=f"{host}:{port}",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        
    def __call__(self):
        Logger.info("Staring call function")
        for message in self.consumer:
            Logger.info("Get Message")
            message = message.value
            print("Message: ", message)

        

