from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest')

print("Nasłuchiwanie transakcji > 1000 PLN...")

for message in consumer:
    tx = message.value
    if tx['amount'] > 1000:
        print(f" ALERT: Wysoka kwota! ID: {tx['tx_id']} | Kwota: {tx['amount']} PLN | Sklep: {tx['store']}")
