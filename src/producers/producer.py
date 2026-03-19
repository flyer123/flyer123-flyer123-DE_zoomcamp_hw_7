import dataclasses
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models import Ride, ride_from_row

# Download NYC yellow taxi trip data (first 1000 rows)
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime',
'lpep_dropoff_datetime',
'PULocationID',
'DOLocationID',
'passenger_count',
'trip_distance',
'tip_amount',
'total_amount']
df = pd.read_parquet(url, columns=columns)

print("Starting to preprocess dataframe")
df["passenger_count"] = df["passenger_count"].fillna(0)
print("Passenger count preprocessed")
df["trip_distance"] = df["trip_distance"].fillna(0)
print("Trip distance preprocessed")
df["tip_amount"] = df["tip_amount"].fillna(0)
print("Tip amount preprocessed")
df["total_amount"] = df["total_amount"].fillna(0)
print("Preprocessing ended")
def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

server = 'redpanda:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)
t0 = time.time()

topic_name = 'green-trips'

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    #print(f"Sent: {ride}")
    time.sleep(0.001)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')