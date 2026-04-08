from quixstreams import Application

# 1. Initialize the App
# In the latest version, 'consumer_group' is used instead of 'group_id'
app = Application(
    broker_address="localhost:9092", 
    consumer_group="fraud-monitor-v1",
    auto_offset_reset="earliest"
)

# 2. Define the Topic
topic = app.topic("transactions", value_deserializer='json')

# 3. Create the Stream Dataframe (sdf)
sdf = app.dataframe(topic)

# 4. Logic: Find transactions where amount is over 5000
fraud_sdf = sdf[sdf["amount"] > 5000]

# 5. Print the result using a simple print update
def report_fraud(row):
    print(f"🚨 ALERT: Potential Fraud detected for {row['user_id']}!")
    print(f"   Details: ${row['amount']} spent at {row['merchant']}\n")

# Apply the function to the filtered stream
fraud_sdf = fraud_sdf.update(report_fraud)

if __name__ == "__main__":
    app.run(sdf)