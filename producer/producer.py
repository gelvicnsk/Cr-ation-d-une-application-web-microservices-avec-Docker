import json
import requests
import time
from kafka import KafkaProducer
import os
from kafka.errors import NoBrokersAvailable

TOPIC = "crypto_prices"
BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print(f"✅ Connecté au broker Kafka : {BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"⛔ Broker {BROKER} non dispo... retry dans 60 sec.")
            time.sleep(60)

producer = create_producer()

url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"

while True:
    try:
        response = requests.get(url, timeout=10)

        if response.status_code == 429:
            print("⛔ Limite CoinGecko atteinte (429). Pause de 2 minutes...")
            time.sleep(120)
            continue

        if response.status_code != 200:
            print(f"⚠️ Erreur API : {response.status_code}")
            time.sleep(60)
            continue

        data = response.json()

        message = {
            "timestamp": time.strftime("%H:%M:%S"),
            "prices": data
        }

        producer.send(TOPIC, message)
        producer.flush()

        print("📤 Message envoyé :", message)

        time.sleep(60)

    except requests.exceptions.RequestException as e:
        print("❌ Erreur réseau :", e)
        time.sleep(60)

    except Exception as e:
        print("❌ Erreur inconnue :", e)
        time.sleep(60)
