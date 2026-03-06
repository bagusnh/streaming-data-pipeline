import json
import time
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'kafka:9092'
}

producer = Producer(conf)

topic = "transactions"

valid_sources = ["mobile", "web", "pos"]


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed:", err)
    else:
        print("Sent:", msg.value().decode())


def generate_valid_event():
    return {
        "user_id": f"U{random.randint(1000,9999)}",
        "amount": random.randint(1000, 500000),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": random.choice(valid_sources)
    }


def generate_invalid_event():
    events = [
        {  # negative amount
            "user_id": "U9999",
            "amount": -100,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "mobile"
        },
        {  # too large amount
            "user_id": "U8888",
            "amount": 99999999,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "web"
        },
        {  # invalid source
            "user_id": "U7777",
            "amount": 2000,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "telegram"
        },
        {  # invalid timestamp
            "user_id": "U6666",
            "amount": 3000,
            "timestamp": "INVALID_TIMESTAMP",
            "source": "mobile"
        }
    ]
    return random.choice(events)


def generate_late_event():
    late_time = datetime.utcnow() - timedelta(minutes=5)

    return {
        "user_id": f"U{random.randint(1000,9999)}",
        "amount": random.randint(1000, 500000),
        "timestamp": late_time.isoformat() + "Z",
        "source": random.choice(valid_sources)
    }


print("Starting producer...")

while True:

    r = random.random()

    if r < 0.6:
        event = generate_valid_event()

    elif r < 0.8:
        event = generate_invalid_event()

    else:
        event = generate_late_event()

    producer.produce(
        topic,
        json.dumps(event),
        callback=delivery_report
    )

    producer.poll(0)

    time.sleep(random.uniform(1, 2))