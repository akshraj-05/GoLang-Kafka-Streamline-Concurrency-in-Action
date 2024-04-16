from confluent_kafka import Producer
import json

f = open('C:/Users/akshraj.chavda/Internship/Go/project/Go_Project/userData.json')
data = json.load(f)
f.close()

KAFKA_PRODUCER_CONFIGURATION = {
    'bootstrap.servers': 'localhost:9092',
}

# Loop 100 times
for _ in range(20):
    producer = Producer(KAFKA_PRODUCER_CONFIGURATION)

    # Produce values
    for i in data:
        producer.produce(topic="userKafka", value=json.dumps(i))
        producer.flush()

    # Producer finished its work
    print("Iteration done")

# All iterations completed
print("All iterations completed")