import os
from dotenv import load_dotenv
import time
import json
import requests
from google.transit import gtfs_realtime_pb2
from confluent_kafka import Producer

load_dotenv()

BKK_API_KEY = os.getenv("BKK_API_KEY", "")
BKK_URL = f"https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/VehiclePositions.pb?key={BKK_API_KEY}"
KAFKA_TOPIC = "bkk.dev.realtime.raw"

kafka_config = {"bootstrap.servers": "localhost:9092"}
producer = Producer(kafka_config)


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg}")


def fetch_and_produce(debug: bool = False):
    try:
        response = requests.get(BKK_URL)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        for entity in feed.entity:
            if entity.HasField("vehicle"):
                vehicle_data = {
                    "vehicle_id": entity.vehicle.vehicle.id,
                    "trip_id": entity.vehicle.trip.trip_id,
                    "route_id": entity.vehicle.trip.route_id,
                    "latitude": entity.vehicle.position.latitude,
                    "longitude": entity.vehicle.position.longitude,
                    "timestamp": entity.vehicle.timestamp,
                }

                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=str(vehicle_data["vehicle_id"]),
                    value=json.dumps(vehicle_data),
                    callback=delivery_report if debug else None
                )

        producer.flush()
        print("Batch processed and sent to Kafka")
    except Exception as e:
        print(f"Error fetching data: {e}")


if __name__ == "__main__":
    while True:
        fetch_and_produce()
        time.sleep(30)