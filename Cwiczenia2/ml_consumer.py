from kafka import KafkaConsumer, KafkaProducer
import json, requests

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest', 
    group_id='ml-scoring-final',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

API_URL = "http://localhost:8001/score"

print("Konsument ML startuje...")

for message in consumer:
    tx = message.value
    
    is_elec = 1 if tx.get('category') == 'elektronika' else 0
    
    payload = {
        "amount": float(tx.get('amount', 0)),
        "is_electronics": int(is_elec),
        "tx_per_minute": int(tx.get('tx_per_minute', 5))
    }
    
    try:
        r = requests.post(API_URL, json=payload)
        if r.status_code == 200:
            res = r.json()
            status = "FRAUD" if res['is_fraud'] else "OK"
            print(f"Kwota: {payload['amount']} PLN | Kat: {tx.get('category')} | Wynik: {status}")
        else:
            print(f"BŁĄD API {r.status_code}: {r.text}")
    except Exception as e:
        print(f"Błąd: {e}")
