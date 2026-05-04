from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

print("Analiza poziomu ryzyka (Risk Enrichment)... (Ctrl+C aby zatrzymać)")

for message in consumer:
    tx = message.value
    
    if tx['amount'] > 3000:
        risk_level = "HIGH"
    elif tx['amount'] > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    
    print(f"[{risk_level}] {tx['tx_id']} | User: {tx['user_id']} | Kwota: {tx['amount']} PLN")
