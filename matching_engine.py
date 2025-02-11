import json
from kafka import KafkaConsumer
from collections import deque
import threading

# Set up Kafka consumer to subscribe to the "orders" topic
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Simple in-memory order book using deques
buy_orders = deque()   # Sorted by descending price (highest bid first)
sell_orders = deque()  # Sorted by ascending price (lowest ask first)


def match_orders():
    """Simple matching algorithm based on price/time priority."""
    while True:
        # Only match if there is at least one buy and one sell order
        if buy_orders and sell_orders:
            best_buy = buy_orders[0]
            best_sell = sell_orders[0]
            # Check if orders can be matched (for limit orders, buy price >= sell price)
            if best_buy['price'] >= best_sell['price']:
                # Determine the matched quantity (minimum of both orders)
                quantity_matched = min(
                    best_buy['quantity'], best_sell['quantity'])
                print(
                    f"Match found: {quantity_matched} units at price {best_sell['price']}")
                # Update order quantities
                best_buy['quantity'] -= quantity_matched
                best_sell['quantity'] -= quantity_matched

                # Remove fully filled orders
                if best_buy['quantity'] == 0:
                    buy_orders.popleft()
                if best_sell['quantity'] == 0:
                    sell_orders.popleft()
            else:
                # No match is possible; break out to wait for more orders
                break


def process_order(order):
    """Process incoming order and add it to the order book."""
    order_type = order.get('order_type')
    side = order.get('side')
    # For simplicity, we assume all orders here are limit orders.
    # Market order processing would require immediate matching against the order book.
    if side == 'buy':
        # Insert buy order and sort by descending price (higher bids first)
        buy_orders.append(order)
        buy_orders = deque(
            sorted(buy_orders, key=lambda x: x['price'], reverse=True))
    elif side == 'sell':
        # Insert sell order and sort by ascending price (lower asks first)
        sell_orders.append(order)
        sell_orders = deque(sorted(sell_orders, key=lambda x: x['price']))
    else:
        print("Unknown order side")
    # Attempt to match orders after each new order arrives
    match_orders()


def consume_orders():
    """Continuously consume orders from Kafka and process them."""
    for message in consumer:
        order = message.value
        print(f"Received order: {order}")
        process_order(order)
        # For demonstration, print the current order book
        print("Current Buy Orders:", list(buy_orders))
        print("Current Sell Orders:", list(sell_orders))


if __name__ == '__main__':
    # Run the order consumption in a dedicated thread
    t = threading.Thread(target=consume_orders)
    t.start()
