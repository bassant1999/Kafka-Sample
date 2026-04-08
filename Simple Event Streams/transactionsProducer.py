from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    key_serializer=lambda k: k.encode('utf-8'), 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

merchants = ["Starbucks", "Jumia", "Vodafone", "TotalEnergies", "Zara"]

print("🚀 Sending transactions...")

try:
    count = 1
    while True:
        merchant = random.choice(merchants)
        data = {"merchant": merchant, "amount": random.randint(10, 500), "timestamp": time.time()}
        
        producer.send('transactions', key=merchant, value=data)
        print(f"✅ Sent {merchant}")
        time.sleep(0.5)
        count += 1
except KeyboardInterrupt:
    print("Stopping Producer...")
finally:
    producer.flush()
    producer.close()