import csv
import json
import time
import os
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("TOPIC_NAME", "retail_topic")
CSV_PATH = os.getenv("CSV_PATH", "/app/data/data.csv")

# Attendre que Kafka soit prêt
time.sleep(15)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None
)

print(f"[Producer] Connexion à {BOOTSTRAP_SERVERS}, topic={TOPIC}")

with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    count = 0
    for row in reader:
        # Nettoyage basique avant envoi
        message = {
            "InvoiceNo":   row.get("InvoiceNo", "").strip(),
            "StockCode":   row.get("StockCode", "").strip(),
            "Description": row.get("Description", "").strip(),
            "Quantity":    row.get("Quantity", "0").strip(),
            "InvoiceDate": row.get("InvoiceDate", "").strip(),
            "UnitPrice":   row.get("UnitPrice", "0").strip(),
            "CustomerID":  row.get("CustomerID", "").strip(),
            "Country":     row.get("Country", "").strip(),
        }
        producer.send(
            TOPIC,
            key=message["InvoiceNo"],
            value=message
        )
        count += 1
        if count % 500 == 0:
            print(f"[Producer] {count} messages envoyés...")
        time.sleep(0.01)  # ~100 msg/s, ajustable

producer.flush()
print(f"[Producer] Terminé — {count} lignes publiées dans '{TOPIC}'")