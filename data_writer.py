from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from kafka import KafkaProducer
from dotenv import load_dotenv
import json
import os
import time

# Load credentials from .env file (never committed to GitHub)
load_dotenv()

# ---- CoinMarketCap API config ----
prod_url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
    'start': '1',
    'limit': '100',
    'convert': 'USD'
}
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': os.getenv('CMC_API_KEY'),
}

# ---- Confluent Cloud Kafka config ----
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_API_KEY = os.getenv('KAFKA_API_KEY')
KAFKA_API_SECRET = os.getenv('KAFKA_API_SECRET')
KAFKA_TOPIC = 'crypto-prices'
POLL_INTERVAL_SECONDS = 300  # 5 minutes

session = Session()
session.headers.update(headers)


def extract_record(crypto_dict):
    """
    Extract and flatten fields from a single crypto record.
    All fields are at the top level (e.g. 'price' instead of 'quote.USD.price').
    """
    return {
        'id':                 crypto_dict.get('id'),
        'name':               crypto_dict.get('name'),
        'symbol':             crypto_dict.get('symbol'),
        'price':              crypto_dict.get('quote', {}).get('USD', {}).get('price'),
        'last_updated':       crypto_dict.get('quote', {}).get('USD', {}).get('last_updated'),
        'volume_24h':         crypto_dict.get('quote', {}).get('USD', {}).get('volume_24h'),
        'volume_change_24h':  crypto_dict.get('quote', {}).get('USD', {}).get('volume_change_24h'),
        'percent_change_1h':  crypto_dict.get('quote', {}).get('USD', {}).get('percent_change_1h'),
        'percent_change_24h': crypto_dict.get('quote', {}).get('USD', {}).get('percent_change_24h'),
    }

def append_to_csv(json_obj, filePath="example_records.csv"):
    import csv
    fieldnames = [
        'id', 
        'name', 
        'symbol',
        'price',
        'last_updated',
        'volume_24h',
        'volume_change_24h',
        'percent_change_1h',
        'percent_change_24h'
    ]

    file_exists = os.path.exists(filePath)
    file_empty = (not file_exists) or os.path.getsize(filePath) == 0

    with open(filePath, "a", newline="") as csvfile:  # append mode
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Only write header if file is new or empty
        if file_empty:
            writer.writeheader()

        for crypto_dict in json_obj['data']:
            flattened_crypto_dict = extract_record(crypto_dict)
            writer.writerow(flattened_crypto_dict)


def publish_to_kafka(producer, json_obj, topic=KAFKA_TOPIC):
    """
    Publish each crypto record as a separate flattened JSON message to Kafka.
    Uses the crypto symbol (e.g. 'BTC') as the message key so all records
    for the same coin always land in the same Kafka partition.
    """
    records_sent = 0
    for crypto_dict in json_obj['data']:
        record = extract_record(crypto_dict)

        message_key = record['symbol'].encode('utf-8') if record.get('symbol') else None

        producer.send(
            topic,
            key=message_key,
            value=record
        )
        records_sent += 1

    producer.flush()
    print(f"Published {records_sent} records to Kafka topic '{topic}'")


def create_producer():
    """Create and return a Confluent Cloud KafkaProducer instance."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=KAFKA_API_KEY,
        sasl_plain_password=KAFKA_API_SECRET,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3
    )


# Create the Kafka producer once outside the loop so we
# reuse the same connection every 5 minutes instead of
# opening and closing a new one each time
producer = create_producer()

print("Starting data writer -- publishing every 5 minutes. Press Ctrl+C to stop.")

while True:
    try:
        response = session.get(prod_url, params=parameters)
        json_obj = json.loads(response.text)
        publish_to_kafka(producer, json_obj)
        # print("JSON", json_obj)
        # append_to_csv(json_obj)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(f"API request failed: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    time.sleep(POLL_INTERVAL_SECONDS)