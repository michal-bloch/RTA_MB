from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sklepy = ['Warszawa', 'Kraków', 'Gdańsk', 'Wrocław']
kategorie = ['elektronika', 'odzież', 'żywność', 'książki']

def generate_transaction():
    tx_id = f'TX{random.randint(1000,9999)}'
    user_id = f'u{random.randint(1,20):02d}'
    store = random.choice(sklepy)
    timestamp = datetime.now().isoformat()
    
    # Zadanie 1.1: 5% szans na transakcję podejrzaną
    if random.random() < 0.05:
        amount = round(random.uniform(3001.0, 5000.0), 2)
        category = 'elektronika'
        hour = random.randint(0, 5)
        status = "PODEJRZANA"
    else:
        amount = round(random.uniform(5.0, 3000.0), 2)
        category = random.choice(kategorie)
        hour = random.randint(6, 23)
        status = "OK"

    return {
        'tx_id': tx_id,
        'user_id': user_id,
        'amount': amount,
        'store': store,
        'category': category,
        'hour': hour,
        'timestamp': timestamp,
        'debug_status': status
    }

print("Uruchamiam wysyłanie transakcji do Kafki... (Ctrl+C aby przerwać)")

try:
    for i in range(1000):
        tx = generate_transaction()
        
        status_info = tx.pop('debug_status')
        
        producer.send('transactions', value=tx)
        
        print(f"[{i+1}] {status_info:10} | {tx['tx_id']} | {tx['amount']:8.2f} PLN | {tx['category']:12} | Godz: {tx['hour']}")
        
        time.sleep(0.5)
except KeyboardInterrupt:
    print("\nZatrzymano przez użytkownika.")
finally:
    producer.flush()
    producer.close()
    print("Połączenie z Kafką zamknięte.")
