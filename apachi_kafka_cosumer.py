from kafka import KafkaConsumer


consumer = KafkaConsumer(
    'rawdata',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     #enable_auto_commit=True,
     #group_id='my-group',
     value_deserializer=lambda x:x.decode('utf-8'))
counter = 0
for message in consumer:
    message = message.value
    counter+=1
    print("\n\n================================  "+ str(counter) +"  ================================\n\n")
    print(message)
    
