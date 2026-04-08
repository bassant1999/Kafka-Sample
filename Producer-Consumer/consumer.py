from kafka import KafkaConsumer
import json
import time

# 1. Initialize the Consumer WITHOUT the automatic value_deserializer
consumer = KafkaConsumer(
    'test-events',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='track-group-01'
)

print("Consumer started... Waiting for messages.")

for message in consumer:
    # message.value is currently raw bytes
    raw_data = message.value.decode('utf-8')
    
    try:
        # Try to parse the string as JSON
        data = json.loads(raw_data)
        print(f"Received JSON: {data['message']} (ID: {data['id']})")
        # time.sleep(5)
    except json.JSONDecodeError:
        # If it's not JSON (the old manual messages), print it as a string
        print(f"Received plain text message: {raw_data}")
    except Exception as e:
        print(f"Unexpected error: {e}")