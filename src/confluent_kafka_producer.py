# -*- coding: utf-8 -*-
"""confluent_kafka.ipynb

To get the colab notebook of the file the below link can be accessed
    https://colab.research.google.com/drive/1Fi5gPQu1cs9BcwqiQFgG4ETWFpv1ZlJ1
"""

import pandas as pd
import json
csv_file = '/content/first_100_customers.csv'
df = pd.read_csv(csv_file)
df.head()

json_records = df.to_dict(orient='records')
json_file = 'customer.json'

with open (json_file, 'w') as file:
    json.dump(json_records,file, indent = 4)
print("File converted to json")

pip install confluent-Kafka

from confluent_kafka import Producer, Consumer
import json
import time

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open('/content/client.properties') as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config
producer = Producer(read_config())

topic = 'ecommerce'
with open('customer.json','r') as file:
    customer_data = json.load(file)
value = customer_data[0]
print(value)

key = value['customer_id']
key_1 =  str(key).encode('utf-8')
value_1 = str(value).encode('utf-8')
producer.produce(topic,key = key_1,value = value_1)

customer_data

def delivery_status(err,msg):
  if(err):
    print(f"message delivery failed {err}")
  else:
    print(f"Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")


for record in customer_data:
  try:
    message_value = json.dumps(record)
    message_key = str(int(record['customer_id']) + 1000).encode('utf-8')
    producer.produce(topic, key = message_key, value = message_value, callback = delivery_status)
    producer.poll(1)

  except Exception as e:
    print(f"error seding message: {e}")

producer.flush()

print("Message sent to kafka succesfully")

