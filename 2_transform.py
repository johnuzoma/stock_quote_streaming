import faust
from datetime import datetime, timezone

app = faust.App(
    'stock-quote-streaming',
    broker="kafka://localhost:9092"#,
)

# Define the schema for input
class RawStockQuote(faust.Record):
    s: str
    p: float
    v: int
    t: int

# Define the schema for output
class TransformedStockQuote(faust.Record):
    symbol: str
    price: float
    volume: int
    timestamp: str 

# Define input and output topics
input_topic = app.topic("stock_quotes", value_type=RawStockQuote)
output_topic = app.topic("transformed_stock_quotes", value_type=TransformedStockQuote)

# Transformation Function
@app.agent(input_topic)
async def transform_stock_quotes(stream):
    async for raw_stock in stream:
        # Transform the fields and convert timestamp to human-readable format
        human_readable_timestamp = datetime.fromtimestamp(raw_stock.t / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        transformed_stock = TransformedStockQuote(
            symbol=raw_stock.s,
            price=raw_stock.p,
            volume=raw_stock.v,
            timestamp=human_readable_timestamp
        )
        print(f"Transformed Stock: {transformed_stock}")
        await output_topic.send(value=transformed_stock)

if __name__ == '__main__':
    app.main()