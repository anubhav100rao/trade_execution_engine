import bisect
import datetime
from collections import deque


class Order:
    def __init__(self, order_id, order_type, price, quantity, timestamp):
        self.order_id = order_id
        self.order_type = order_type  # 'buy' or 'sell'
        self.price = price
        self.quantity = quantity
        self.timestamp = timestamp


class MatchingEngine:
    def __init__(self):
        self.bids = {}      # Price -> deque of buy orders (sorted descending)
        self.asks = {}      # Price -> deque of sell orders (sorted ascending)
        self.bid_prices = []  # Stored as negative values for ascending sort
        self.ask_prices = []  # Stored as positive values
        self.trades = []

    def add_order(self, order):
        if order.order_type == 'buy':
            self._match_buy_order(order)
            if order.quantity > 0:
                self._add_to_book(order, self.bids,
                                  self.bid_prices, is_bid=True)
        else:
            self._match_sell_order(order)
            if order.quantity > 0:
                self._add_to_book(order, self.asks,
                                  self.ask_prices, is_bid=False)

    def _add_to_book(self, order, book, price_list, is_bid):
        price = order.price
        stored_price = -price if is_bid else price

        # Find insertion point using bisect
        index = bisect.bisect_left(price_list, stored_price)

        # Get actual price for book storage
        actual_price = -stored_price if is_bid else stored_price

        if index < len(price_list) and price_list[index] == stored_price:
            # Existing price level
            book[actual_price].append(order)
        else:
            # New price level
            price_list.insert(index, stored_price)
            book[actual_price] = deque([order])

    def _match_buy_order(self, buy_order):
        while self.ask_prices and buy_order.quantity > 0:
            best_ask = self.ask_prices[0]

            if best_ask > buy_order.price:
                break  # No more possible matches

            orders_at_price = self.asks[best_ask]
            while orders_at_price and buy_order.quantity > 0:
                sell_order = orders_at_price[0]
                trade_qty = min(buy_order.quantity, sell_order.quantity)

                # Execute trade
                self._execute_trade(sell_order, buy_order, trade_qty, best_ask)

                # Update quantities
                sell_order.quantity -= trade_qty
                buy_order.quantity -= trade_qty

                if sell_order.quantity == 0:
                    orders_at_price.popleft()

            # Cleanup empty price levels
            if not orders_at_price:
                del self.asks[best_ask]
                self.ask_prices.pop(0)

    def _match_sell_order(self, sell_order):
        while self.bid_prices and sell_order.quantity > 0:
            best_bid_stored = self.bid_prices[0]
            best_bid = -best_bid_stored  # Convert stored price to actual

            if best_bid < sell_order.price:
                break  # No more possible matches

            orders_at_price = self.bids[best_bid]
            while orders_at_price and sell_order.quantity > 0:
                buy_order = orders_at_price[0]
                trade_qty = min(sell_order.quantity, buy_order.quantity)

                # Execute trade
                self._execute_trade(sell_order, buy_order, trade_qty, best_bid)

                # Update quantities
                buy_order.quantity -= trade_qty
                sell_order.quantity -= trade_qty

                if buy_order.quantity == 0:
                    orders_at_price.popleft()

            # Cleanup empty price levels
            if not orders_at_price:
                del self.bids[best_bid]
                self.bid_prices.pop(0)

    def _execute_trade(self, sell_order, buy_order, quantity, price):
        self.trades.append({
            'timestamp': datetime.datetime.now(),
            'price': price,
            'quantity': quantity,
            'buy_order': buy_order.order_id,
            'sell_order': sell_order.order_id
        })
        print(f"Trade executed: {quantity}@{price} | "
              f"Buy: {buy_order.order_id}, Sell: {sell_order.order_id}")


# Example usage
if __name__ == "__main__":
    engine = MatchingEngine()

    # Add some orders
    now = datetime.datetime.now()
    engine.add_order(Order(1, 'sell', 100, 50, now))
    engine.add_order(Order(2, 'sell', 101, 30, now))
    engine.add_order(Order(3, 'buy', 102, 70, now))

    print("\nRemaining asks:")
    for price in sorted(engine.asks.keys()):
        print(
            f"Price {price}: {sum(order.quantity for order in engine.asks[price])} shares")

    print("\nRemaining bids:")
    for price in sorted(engine.bids.keys(), reverse=True):
        print(
            f"Price {price}: {sum(order.quantity for order in engine.bids[price])} shares")
