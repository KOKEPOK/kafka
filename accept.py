import json
import datetime
import random
import time
from kafka import KafkaConsumer
from kafka import KafkaProducer
import os

consumer = KafkaConsumer('main_topic', 	bootstrap_servers=['192.168.56.1:58853'], api_version=(0,10,1), auto_offset_reset='earliest')
producer = KafkaProducer(				bootstrap_servers=['192.168.56.1:58853'], api_version=(0,10,1))

print("*"*50,"\nПрограмма вычитает из топика kafka сообщения.\nЕсли вычитанное сообщение типа message, то в консоль пишется Done! \nЕсли вычитанное сообщение не типа  message, то оно помещается в dead letter.")
print("*"*50)

try:
	last_offset = int(open("last_offset.txt", "r").read())
except FileNotFoundError:
	last_offset = 0
try:
	while 1:
		for msg in consumer:
			if msg.offset > last_offset:
				type_message = (json.loads(msg.value.decode('utf-8')))['type']
				if type_message == 'message':
					print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tDone")

				else:
					print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\t -> в другой топик топай!")
					send_json = (json.dumps({"type": type_message})).encode('utf-8')
					producer.send('dead_letter', send_json)

				last_offset = msg.offset
		
except KeyboardInterrupt:
	file = open("last_offset.txt", "w")
	file.write(str(last_offset))
	file.close()
	print("\n")
	print("*"*50,"\nДо встречи, бро!\nNikel, 2020 ")