from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'testing',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
print("====================111111111=============")
for message in consumer:
    print("=============222222222222===============")
    message = message.value
    print('-----------------22222222222----------------------')
    print(str(message)+ '\nTimestamp:'+str(time.ctime(time.time()))) 

