from faker import Faker
import random
from kafka import KafkaProducer
import json
from time import sleep
import time
fake = Faker()

# Tạo danh sách các nhãn
labels = 'face'

# Tạo danh sách các nhóm tìm kiếm
search_groups = ['group1', 'group2', 'group3']

# Tạo danh sách các user id
user_ids = list(range(1, 101))

# Tạo danh sách các tọa độ xyxy và khoảng cách so khớp
bbox = []
distances = []
for i in range(100):
    x1 = random.randint(0, 100)
    y1 = random.randint(0, 100)
    x2 = random.randint(101, 200)
    y2 = random.randint(101, 200)
    bbox.append({'x1':x1,'y1':y1,'x2':x2,'y2':y2})
    distances.append(random.uniform(0, 1))

# Tạo danh sách các track id
track_ids = [fake.uuid4() for _ in range(100)]

# Tạo danh sách các ảnh/video event
event_links = [fake.url() for _ in range(100)]

# Tạo danh sách các face với thông tin ngẫu nhiên
faces = []
for i in range(100):
    face = {
        'bbox': bbox[i],
        'distance': distances[i],
        'label': labels,
        'track_id': track_ids[i],
        'user_id': random.choice(user_ids),
        'search_group': random.choice(search_groups),
        'event_link': event_links[i]
    }
    faces.append(face)

# print(faces)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # Encode all values as JSON
    value_serializer=lambda value: json.dumps(value).encode(),
)
count = 0
for i in faces:
  producer.send('testing', value=i)
  print('--------------------------------------------------', count)
  print(str(i)+ '\nTimestamp:' + time.ctime(time.time()))  # DEBUG
  count += 1
  sleep(1)