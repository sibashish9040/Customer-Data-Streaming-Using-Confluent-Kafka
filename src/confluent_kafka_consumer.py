from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import time

# As for consuming there are extra parameters we have to add so here we have added new paramteres
#group.id and auto.offset.reset are the new added parameters
conf ={
  "bootstrap.servers":"pkc-921jm.us-east-2.aws.confluent.cloud:9092",
  "security.protocol":"SASL_SSL",
  "sasl.mechanisms":"PLAIN",
  "sasl.username":"FLY6JUGGUCNYPEC4",
  "sasl.password":"ag/kH0DDkMvDUIt3zscBtcDhct9ZrzzQjzFIzNIMcgGIkbKyc7ZTPQ/dF6tOGnoO",
  'session.timeout.ms': 45000,
  'client.id': 'ccloud-python-client-0ba89b25-4c4f-437d-b6a2-47efab204258'  ,
  'auto.offset.reset' : 'earliest',
  'group.id': 'customer_group'
}
consumer=  Consumer(conf)

topic = 'ecommerce'
consumer.subscribe([topic])

def process_message(message):
  try:
    if message.error():
      if(message.error.code()) == KafkaError._PARTITION_EOF:
        print('End of partition reached {0}/{1}')
      else:
        raise KafkaException(message.error())
    else:
      key = message.key().decode('utf-8')
      value = json.loads(message.value().decode('utf-8'))

      print(f"Recieved message : Key {key} , Value : {value}")
  except Exception as e:
    print(f"Error consuming/processing message : {e}")


# Poll messages Continously
try :
  print(" Listening for messages, press Ctrl + C to exit")

  while True:
    message = consumer.poll(timeout=1.0)
    if(message):
      process_message(message)

except KeyboardInterrupt:
  print("Interrupted by user, shutting down consumer")

finally:
  consumer.close()
