import os
import requests
import json
import Geohash
from pykafka import KafkaClient

sample_file = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-01.csv"
kafka_brokers = os.getenv("KAFKA_BROKERS", "192.168.56.222:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "drivers_topic")

client = KafkaClient(hosts=kafka_brokers)
topic = client.topics[kafka_topic.encode("utf8")]
producer = topic.get_producer()

if not os.path.isfile(sample_file):
  url = sample_file
  response = requests.get(url, stream=True)

  for line in response.iter_lines():
    if len(line) > 10 and line.split(",")[0].isdigit():
      data = {}
      if line.split(",")[0] == "1":
        data["ID"] = line.split(",")[0]
        data["current_time"] = line.split(",")[1]
        driver_long = line.split(",")[5]
        driver_lat = line.split(",")[6]
        data["driver_geohash"] = Geohash.encode(float(driver_lat), float(driver_long), precision=6)
        json_data = json.dumps(data)
        producer.produce(json_data)

