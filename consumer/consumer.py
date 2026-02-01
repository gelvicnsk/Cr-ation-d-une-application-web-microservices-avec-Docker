import json
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from collections import deque
import os
import time
from kafka.errors import NoBrokersAvailable
import socket

TOPIC = "crypto_prices"


def detect_broker():
   
    env_broker = os.getenv("KAFKA_BROKER")
    if env_broker:
        print(f"🔧 Broker défini par variable d'environnement : {env_broker}")
        return env_broker
    
    try:
        socket.gethostbyname("kafka")
        print("🐳 Environnement Docker détecté → utilisation de kafka:29092")
        return "kafka:29092"
    except socket.gaierror:
        pass

    print("💻 Exécution locale détectée → utilisation de localhost:9092")
    return "localhost:9092"


BROKER = detect_broker()
def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BROKER],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='crypto_group_1',  
                enable_auto_commit=True
            )
            print(f"✅ Connecté au broker Kafka : {BROKER}")
            return consumer
        except NoBrokersAvailable:
            print(f"⚠️ Broker {BROKER} non disponible, nouvelle tentative dans 5 secondes...")
            time.sleep(5)


consumer = create_consumer()
window = 20
btc_prices = deque(maxlen=window)
eth_prices = deque(maxlen=window)
timestamps = deque(maxlen=window)
plt.ion()
fig, ax = plt.subplots()
plt.title("Évolution des prix des cryptomonnaies (USD)")
plt.xlabel("Temps")
plt.ylabel("Prix ($)")

btc_line, = ax.plot([], [], label='Bitcoin')
eth_line, = ax.plot([], [], label='Ethereum')
plt.legend()

print("📡 En attente de messages Kafka...")
for message in consumer:
    data = message.value
    print("📥 Message reçu :", data)

    timestamp = data.get("timestamp")
    prices = data.get("prices", {})

    if "bitcoin" not in prices or "ethereum" not in prices:
        print("⚠️ Donnée invalide, ignorée :", prices)
        continue

    btc_price = prices["bitcoin"]["usd"]
    eth_price = prices["ethereum"]["usd"]

    btc_prices.append(btc_price)
    eth_prices.append(eth_price)
    timestamps.append(timestamp)

    btc_line.set_data(range(len(btc_prices)), btc_prices)
    eth_line.set_data(range(len(eth_prices)), eth_prices)

    ax.relim()
    ax.autoscale_view()
    plt.xticks(rotation=45)
    plt.pause(0.5)
