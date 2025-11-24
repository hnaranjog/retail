import time 
import json 
import random 
from kafka import KafkaProducer 

def generate_sensor_data(): 
    # El cuerpo de la función SÍ debe estar indentado
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time())
        } 


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], 
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
) 

while True: 
    try:
        sensor_data = generate_sensor_data() 
        # Envío asíncrono. Asegúrate de manejar la conexión correctamente.
        producer.send('sensor_data', value=sensor_data)
        print(f"Sent: {sensor_data}") 
        time.sleep(1)
    except Exception as e:
        # En caso de que Kafka no esté disponible
        print(f"Error sending data: {e}")
        time.sleep(5)