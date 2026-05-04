from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0
    rules = []
    # R1:
    if tx.get('amount', 0) > 3000:
        score += 3
        rules.append("R1")
    # R2:
    if tx.get('category') == 'elektronika' and tx.get('amount', 0) > 1500:
        score += 2
        rules.append("R2")
    # R3:
    if tx.get('hour') is not None and tx.get('hour') < 6:
        score += 2
        rules.append("R3")
    return score, rules

print("--- System Fraud Detection START ---")

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    
    if score >= 3:
        tx['fraud_score'] = score
        tx['rules'] = rules
        alert_producer.send('alerts', value=tx)
        print(f" ALERT! Score: {score} | TX: {tx['tx_id']} | Reguły: {rules}")
    else:
        print(f" OK: {tx['tx_id']} (Score: {score})")
