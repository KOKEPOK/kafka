import json
import datetime
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['192.168.56.1:58853'], api_version=(0,10,1))
print("*"*50,"\nПосылает в общий топик kafka сообщения.\nНеобходимо выбрать тип сообщений для отправки его в топик.")
print("*"*50)

try:
	while 1:
		send_json = None
		try:
			send_type_message = int(input("\t1 - 'message'\n\t2 - 'error'\n"))
		except ValueError:
			continue

		if send_type_message == 1:
			print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tОтправка сообщения типа message")
			send_json = (json.dumps({"type": "message"})).encode('utf-8')
			
		else:
			if send_type_message == 2:
				print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),"\tОтправка сообщения типа error")
				send_json = (json.dumps({"type": "error"})).encode('utf-8')

			else:
				other_type_message = str(input("Вероятно, Вы хотите ввести свой тип сообщения? Введите его сейчас: "))
				print(datetime.datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S"),("\tОтправка сообщения типа *{}*".format(other_type_message)))
				send_json = (json.dumps({"type": other_type_message})).encode('utf-8')

		producer.send('main_topic', send_json)
		
except KeyboardInterrupt:
	print("\n")
	print("*"*50,"\nВсего доброго!")

