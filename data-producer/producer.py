import json
import time
import random
import os
import socket
from confluent_kafka import Producer

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_PAGEVIEW", "pageviews")

POSTCODES = ['SW19', 'E1 6AN', 'W1A 1AA', 'M1 1AE', 'B1 1BB', 'EH1 1YZ']
WEBPAGES = [
    'www.website.com/index.html',
    'www.website.com/about.html',
    'www.website.com/products.html',
    'www.website.com/contact.html',
    'www.website.com/checkout.html'
]


def wait_for_kafka(broker_string):
    """Pings the Kafka port until it is open and ready to accept connections."""
    host, port = broker_string.split(':')
    print(f"Waiting for Kafka to wake up at {host}:{port}...")

    while True:
        try:
            with socket.create_connection((host, int(port)), timeout=1):
                print("Kafka is awake and listening! Starting producer...")
                break
        except OSError:
            print("Kafka not ready yet, sleeping for 2 seconds...")
            time.sleep(2)


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def generate_event():
    """Generates a mock pageview event based on the task requirements."""
    return {
        'user id': random.randint(1, 10000),
        'postcode': random.choice(POSTCODES),
        'webpage': random.choice(WEBPAGES),
        'timestamp': int(time.time())
    }


def main():
    wait_for_kafka(KAFKA_BROKER)

    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'pageview-producer'
    }

    producer = Producer(conf)
    print(f"Sending data to topic '{KAFKA_TOPIC}' on {KAFKA_BROKER}...")

    try:
        while True:
            event = generate_event()

            producer.produce(
                topic=KAFKA_TOPIC,
                value=json.dumps(event).encode('utf-8'),
                callback=delivery_report
            )

            producer.poll(0)
            time.sleep(random.uniform(0.5, 1.5))

    except KeyboardInterrupt:
        print("\nStopping producer gracefully...")
        pass
    finally:
        print("Flushing remaining messages...")
        producer.flush(timeout=5)
        print("Producer shutdown complete.")


if __name__ == '__main__':
    main()