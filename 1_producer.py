import json
from kafka import KafkaProducer
import websocket
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'stock_quotes'

# Finnhub Configuration
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
STOCK_SYMBOLS = ['META', 'AMZN']

# Initialize Kafka Producer
"""
KafkaProducer: This creates a Kafka producer, which is responsible for sending messages (data) to a Kafka topic.
bootstrap_servers=KAFKA_BROKER: Specifies the Kafka broker's address (localhost:9092). This is the service Kafka runs to accept connections and manage topics.
value_serializer: Defines a function that transforms the data before sending it to Kafka.
Here, json.dumps(v) converts the data (v) into a JSON string.
.encode('utf-8') ensures the JSON string is encoded as UTF-8, which is the expected format for Kafka messages.
"""
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# WebSocket Message Handling
"""
This function is triggered every time the WebSocket receives a message.
json.loads(message): Parses the incoming WebSocket message (JSON string) into a Python dictionary.
if 'data' in data: Checks if the message contains a key 'data'. This is where Finnhub sends stock quotes.
Loop through quotes:
For each stock quote in data['data'], print it to the console.
Use producer.send(KAFKA_TOPIC, value=quote) to send the quote to the Kafka topic stock_quotes.
"""
def on_message(ws, message):
    data = json.loads(message)
    if 'data' in data:
        for quote in data['data']:
            print(f"Publishing to Kafka: {quote}")
            producer.send(KAFKA_TOPIC, value=quote)

"""
This function handles errors that occur during WebSocket communication.
Logs the error to the console.
"""
def on_error(ws, error):
    print(f"WebSocket error: {error}")

"""
This function runs when the WebSocket connection is closed.
Logs a message indicating the WebSocket has been closed.
"""
def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code}, {close_msg}")

"""
This function runs when the WebSocket connection is opened.
It subscribes to real-time updates for the stock symbols (META, AMZN).
ws.send(json.dumps(...)): Sends a message to the WebSocket server, telling Finnhub to provide updates for the specified stock symbols.
"""
def on_open(ws):
    # Subscribe to stock symbols
    for symbol in STOCK_SYMBOLS:
        ws.send(json.dumps({'type': 'subscribe', 'symbol': symbol}))

# Connect to Finnhub WebSocket
"""
websocket.WebSocketApp: Creates a WebSocket client connection to Finnhub’s WebSocket server (wss://ws.finnhub.io?token=<your_token>).
Links the following functions to handle specific events:
on_message: Triggered when a message is received.
on_error: Triggered when an error occurs.
on_close: Triggered when the WebSocket is closed.
on_open: Sets up subscriptions to stock symbols when the WebSocket connection opens.
"""
ws = websocket.WebSocketApp(
    f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}",
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)
ws.on_open = on_open

# Start WebSocket
"""
ws.run_forever(): Starts the WebSocket client and keeps it running indefinitely to receive real-time stock updates.
try/except KeyboardInterrupt: Allows you to stop the WebSocket client gracefully by pressing Ctrl+C. 
When interrupted, it prints "Stopping WebSocket...".
Closes the WebSocket connection with ws.close().
"""
try:
    print("Starting WebSocket connection...")
    ws.run_forever()
except KeyboardInterrupt:
    print("Stopping WebSocket...")
    ws.close()
