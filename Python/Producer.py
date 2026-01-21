import sys
from confluent_kafka import Producer

class PYProducer:
    def __init__(self, config):
        self.producer = Producer(config)

    def send(self, topic, key, value):
        self.producer.produce(topic, key=key, value=value, callback=self.status)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

    def status(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to partition [{msg.partition()}]")


if __name__ == "__main__":
    args = sys.argv[1:]
    args = " ".join(args)

    client = PYProducer({'bootstrap.servers': 'localhost:9092'})
    client.send('testtopic', None, args)
    client.flush()
