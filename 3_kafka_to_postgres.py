import json
import psycopg2
from kafka import KafkaConsumer

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'transformed_stock_quotes'

# PostgreSQL Configuration
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'store'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Process Messages from Kafka
try:
    print(f"Listening to Kafka topic '{KAFKA_TOPIC}'...")
    for message in consumer:
        # Extract the message value
        data = message.value
        print(f"Received message: {data}")

        # Insert data into PostgreSQL
        cursor.execute("""
            INSERT INTO stock_quotes (symbol, price, volume, timestamp)
            VALUES (%s, %s, %s, %s)
        """, (data['symbol'], data['price'], data['volume'], data['timestamp']))
        conn.commit()

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    # Close database connection
    cursor.close()
    conn.close()
    print("Database connection closed.")
