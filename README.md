# 📈 Stock Quote Streaming Pipeline

A real-time data pipeline that streams live stock quotes from the Finnhub API, publishes them to Kafka, transforms them using Faust, and (optionally) stores them in a PostgreSQL database.

## Overview

This project demonstrates a full streaming data pipeline built in Python using:

- **Finnhub WebSocket API** for real-time stock quotes
- **Apache Kafka** as the message broker and storage for raw/transformed data
- **Faust** for stream processing and data transformation
- **(Optional)** PostgreSQL for storage of transformed data
- **Docker** to containerize all components, ensuring reproducibility

## Data Flow

<img width="683" alt="image" src="https://github.com/user-attachments/assets/edb88f63-c8ff-497d-8f7a-c6c07cd5bd3a" />

## Setup Instructions

1. Clone the Repo

    ```
    git clone https://github.com/your-username/stock_quote_streaming.git
    cd stock_quote_streaming
    ```

2. Create and Populate .env
   
    `echo "FINNHUB_API_KEY=your_finnhub_api_key_here" > .env`
    
    **Note**: Ensure .env is added to .gitignore file

3. Install Dependencies
    Create a virtual environment and install requirements:
    ```
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    pip install -r requirements.txt
    ```

4. Start Kafka & Zookeeper
   
    `docker-compose up -d`
    
    Kafka will be available on localhost:9092

5. Run the Pipeline

    a. Start the Kafka Producer
    
    `python producer.py`

    b. Run Faust Stream Processor

    `faust -A transform worker -l info`
    
    Faust will consume from the stock_quotes topic and publish to transformed_stock_quotes.

    Example Output
    
    ```
    Publishing to Kafka: {'s': 'META', 'p': 476.5, 'v': 20, 't': 1712345678901}
    Transformed Stock: TransformedStockQuote(symbol='META', price=476.5, volume=20, timestamp='2025-
    ```

