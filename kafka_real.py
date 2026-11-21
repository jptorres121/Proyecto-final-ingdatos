"""Productor y consumidor opcionales contra un broker Apache Kafka real.
Requiere: pip install kafka-python y un broker iniciado con docker compose.
"""
import argparse
import json
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

BROKER = "localhost:9092"
TOPIC = "spotify.reproducciones"


def producir(cantidad=10):
    producer = KafkaProducer(bootstrap_servers=BROKER,
                             value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                             key_serializer=lambda key: key.encode("utf-8"))
    try:
        for i in range(cantidad):
            evento = {"evento_id": f"KFK-{int(time.time())}-{i}", "artista": ["Adele", "Coldplay", "Shakira"][i % 3],
                      "cancion": f"Cancion demo {i+1}", "reproducciones": (i + 1) * 100,
                      "event_time": datetime.now(timezone.utc).isoformat()}
            futuro = producer.send(TOPIC, key=evento["artista"], value=evento)
            metadata = futuro.get(timeout=15)
            print(f"Publicado {evento['evento_id']} en partición={metadata.partition}, offset={metadata.offset}")
        producer.flush()
    finally:
        producer.close()


def consumir(maximo=10):
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=BROKER, group_id="spotify-manual-tecnico",
                             auto_offset_reset="earliest", enable_auto_commit=True,
                             value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                             consumer_timeout_ms=10000)
    total = 0
    try:
        for mensaje in consumer:
            print(f"partición={mensaje.partition} offset={mensaje.offset} clave={mensaje.key!r} evento={mensaje.value}")
            total += 1
            if maximo and total >= maximo:
                break
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cliente Python para Kafka real")
    parser.add_argument("accion", choices=["producir", "consumir"])
    parser.add_argument("--cantidad", type=int, default=10)
    args = parser.parse_args()
    producir(args.cantidad) if args.accion == "producir" else consumir(args.cantidad)
