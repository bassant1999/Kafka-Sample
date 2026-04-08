from kafka import KafkaProducer
import json
import time

# 1. Initialize the Producer
# 'localhost:9092' works because your Docker container maps that port to your host
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Starting Producer... Press Ctrl+C to stop.")

try:
    count = 1
    while True:
        data = {
            "id": count,
            "message": f"Hello from Python to Docker Kafka!",
            "timestamp": time.time()
        }
        
        # 2. Send data to the 'test-events' topic we created earlier
        producer.send('test-events', value=data)
        print(f"Sent: {data}")
        
        count += 1
        time.sleep(2) # Send a message every 2 seconds
except KeyboardInterrupt:
    print("Stopping Producer...")
finally:
    producer.close()