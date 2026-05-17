import json
import time
import random
import urllib.request
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC  = "crypto-trades"

SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "BNB-USD"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
)

def fetch_price(symbol):
    url = f"https://api.coinbase.com/v2/prices/{symbol}/spot"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read())
            return float(data["data"]["amount"])
    except Exception as e:
        print(f"[WARN] Could not fetch {symbol}: {e}")
        return None

trade_id = 1000000
print("[STARTED] Live Coinbase price stream -> Kafka")

while True:
    for symbol in SYMBOLS:
        price = fetch_price(symbol)
        if price is None:
            continue

        trade = {
            "event_type":  "trade",
            "event_time":  int(time.time() * 1000),
            "symbol":      symbol.replace("-", ""),
            "trade_id":    trade_id,
            "price":       price,
            "quantity":    round(random.uniform(0.001, 2.5), 6),
            "buyer_maker": random.choice([True, False]),
            "trade_time":  int(time.time() * 1000),
        }

        producer.send(KAFKA_TOPIC, value=trade)
        print(f"[SENT] {trade['symbol']} | Price: {trade['price']}")
        trade_id += 1

    time.sleep(1)
