import dataclasses
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

# Load dataset
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

columns = [
    'lpep_pickup_datetime',
    'PULocationID',
    'DOLocationID',
    'trip_distance'
]

df = pd.read_parquet(url, columns=columns)

# Basic cleanup
df["trip_distance"] = df["trip_distance"].fillna(0)
df["PULocationID"] = df["PULocationID"].fillna(0).astype(int)

# Serializer
def json_serializer(obj):
    return json.dumps(obj).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers=["redpanda:9092"],
    key_serializer=json_serializer,
    value_serializer=json_serializer
)

topic = "green-trips"

print("Starting producer...")

for _, row in df.iterrows():
    event = {
        "PULocationID": int(row["PULocationID"]),
        "DOLocationID": int(row["DOLocationID"]),
        "trip_distance": float(row["trip_distance"]),
        "lpep_pickup_datetime": str(row["lpep_pickup_datetime"])
    }

    # 🔑 KEY = PULocationID (critical for state distribution)
    key = {"kafka_key": str(event["PULocationID"])}

    producer.send(topic, key=key, value=event)
    time.sleep(0.01)

producer.flush()

print("Finished producing.")