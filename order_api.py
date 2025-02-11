from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json

app = FastAPI()

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ORDER_TOPIC = "orders"

# Define request schema


class OrderRequest(BaseModel):
    order_id: str
    side: str  # "buy" or "sell"
    price: float = None  # Optional for market orders
    quantity: int
    order_type: str  # "limit" or "market"


@app.post("/submit_order")
async def submit_order(order: OrderRequest):
    order_dict = order.dict()
    producer.send(ORDER_TOPIC, order_dict)
    producer.flush()
    return {"status": "Order submitted", "order": order_dict}
