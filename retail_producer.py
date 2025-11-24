from kafka import KafkaProducer
from datetime import datetime
import json
import random
import time


def create_producer():
    """
    Crea un productor de Kafka que envía mensajes en formato JSON
    al broker en localhost:9092.
    """
    producer = KafkaProducer(
        bootstrap_servers=["VMBIGDATA:9092", "127.0.0.1:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(3, 8, 0)  # Ajustar a la versión de Kafka instalada (3.8.x)
    )
    return producer


def generate_event():
    """
    Genera un evento de venta simulado.
    Ajusta los valores según tu contexto.
    """
    products = ["10001", "10002", "10003", "10004", "10005"]
    countries = ["United Kingdom", "Germany", "France", "Spain", "Netherlands"]

    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "product": random.choice(products),
        "country": random.choice(countries),
        "quantity": random.randint(1, 10),
        "amount": round(random.uniform(5, 200), 2)
    }
    return event


def main():
    producer = create_producer()
    topic = "retail-stream"

    print(f"Enviando eventos al topic '{topic}'... (Ctrl+C para detener)")

    try:
        while True:
            event = generate_event()
            producer.send(topic, event)
            print("Evento enviado:", event)
            time.sleep(1)  # un evento por segundo
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario. Cerrando productor...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()

