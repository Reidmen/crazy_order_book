from collections import namedtuple, deque
from sortedcontainers import SortedDict
from decimal import Decimal
import itertools
import time

# Order structure
Order = namedtuple("Order", ["order_id", "side", "price", "quantity", "timestamp"])


class LimitOrderBook:
  """
  An efficient implementation of the Limit Order Book (LOB)

  * Using SortedDict for price level management O(log(N))
  # Deque for FIFO order queues with price levels O(1)
  * Order lookup for cancellation O(1) on average (dict)
  """

  def __init__(self) -> None:
    # Bids: SortedDict mapping price -> deque of Orders
    # Bids access highest element bids.peekitem(-1)
    self.bids = SortedDict()
    # Asks: SortedDict mapping price -> deque of Orders
    # Asks access lowest element asks.peekitem(0)
    self.asks = SortedDict()

    # Orders lookup table mapping order_id -> Order object
    self._orders = {}
    self.trades = []

    # Counter for generating unique order IDs
    self._order_id_counter = itertools.count(1)

    print("Limit Order Book initialized")

  def _add_order_to_book(self, order: Order) -> None:
    side_book = self.bids if order.side == "BUY" else self.asks

    # Add price level to the orderbook side
    if order.price not in side_book:
      side_book[order.price] = deque()

    # Add new order to the price level
    side_book[order.price].append(order)
    self._orders[order.order_id] = order  # Store for quick lookup

  def _record_trade(
    self,
    price: Decimal,
    quantity: Decimal,
    maker_order_id: int,
    taker_order_id: int,
    timestamp: float,
  ):
    self.trades.append(
      {
        "price": price,
        "quantity": quantity,
        "timestamp": timestamp,
        "maker_order_id": maker_order_id,
        "taker_order_id": taker_order_id,
      }
    )

  def add_order(
    self, side: str, price: Decimal, quantity: Decimal
  ) -> tuple[int, list[dict[str, Decimal | float | int]]]:
    if side not in ("BUY", "SELL"):
      raise ValueError("Side must be 'BUY' or 'SELL'")
    if not isinstance(price, Decimal) or price <= Decimal("0"):
      raise ValueError("Price must be positive Decimal")
    if not isinstance(quantity, Decimal) or quantity <= Decimal("0"):
      raise ValueError("Quantity must be a positive Decimal")

    order_id = next(self._order_id_counter)
    timestamp = time.time()
    order = Order(order_id, side, price, quantity, timestamp)
    executed_trades = []

    # Matching logic
    remaining_quantity = order.quantity

    if order.side == "BUY":
      # Match against asks (lower first)
      while (
        remaining_quantity > 0 and self.asks and order.price >= self.asks.peekitem(0)[0]
      ):
        best_ask_price, ask_orders_at_level = self.asks.peekitem(0)  # O(1) peek

        while ask_orders_at_level and remaining_quantity > 0:
          maker_order = ask_orders_at_level[0]  # FIFO - O(1) access
          assert isinstance(maker_order, Order), "maker_order is not Order"
          trade_quantity = min(remaining_quantity, maker_order.quantity)
          trade_price = maker_order.price

          # Record the trade
          trade_timestamp = time.time()
          # _record_trade appends a new trade in self.trades
          self._record_trade(
            trade_price,
            trade_quantity,
            maker_order.order_id,
            order.order_id,
            trade_timestamp,
          )
          executed_trades.append(self.trades[-1])

          remaining_quantity -= trade_quantity
          new_maker_quantity = maker_order.quantity - trade_quantity

          if new_maker_quantity == 0:
            # Maker order fully filled, remove it
            ask_orders_at_level.popleft()  # O(1)
            del self._orders[maker_order.order_id]
          else:
            ask_orders_at_level[0] = maker_order._replace(quantity=new_maker_quantity)
            self._orders[maker_order.order_id] = ask_orders_at_level[
              0
            ]  # Update order in the lookup table

        if not ask_orders_at_level:
          del self.asks[best_ask_price]  # O(log N)

    elif order.side == "SELL":
      # Match against bids (highest first)
      while (
        remaining_quantity > 0
        and self.bids
        and order.price <= self.bids.peekitem(-1)[0]
      ):
        best_bid_price, bid_orders_at_level = self.bids.peekitem(-1)  # O(1)

        while bid_orders_at_level and remaining_quantity > 0:
          maker_order = bid_orders_at_level[0]  # FIFO - O(1)
          assert isinstance(maker_order, Order), "maker_order is not Order"
          trade_quantity = min(remaining_quantity, maker_order.quantity)
          trade_price = maker_order.price

          trade_timestamp = time.time()
          self._record_trade(
            trade_price,
            trade_quantity,
            maker_order.order_id,
            order.order_id,
            trade_timestamp,
          )
          executed_trades.append(self.trades[-1])

          remaining_quantity -= trade_quantity
          new_maker_quantity = maker_order.quantity - trade_quantity

          if new_maker_quantity == 0:
            # Maker order fully filled, remove it
            bid_orders_at_level.popleft()
            del self._orders[maker_order.order_id]
          else:
            # Update maker order quantity
            bid_orders_at_level[0] = maker_order._replace(quantity=new_maker_quantity)
            self._orders[maker_order.order_id] = bid_orders_at_level[0]

        if not bid_orders_at_level:
          del self.bids[best_bid_price]  # O(log N)

    # Remaining quantity
    if remaining_quantity > 0:
      final_order = order._replace(quantity=remaining_quantity)
      self._add_order_to_book(final_order)

    return order.order_id, executed_trades

  def cancel_order(self, order_id: int) -> bool:
    """Cancels an existing order by its ID"""
    if order_id not in self._orders:
      return False

    order_to_cancel = self._orders[order_id]
    assert isinstance(order_to_cancel, Order)
    side_book = self.bids if order_to_cancel.side == "BUY" else self.asks

    # Check if price level not in the book side
    if order_to_cancel.price not in side_book:
      # Error when book is not consistent
      del self._orders[order_id]
      return False

    order_queue = side_book[order_to_cancel.price]

    # Efficiently remove from deque
    # deques don't have O(1) arbitrary removal. O(k) where k is orders at level.
    # Usually k is small, so this is acceptable.
    try:
      assert isinstance(order_queue, deque), "orders queue must be 'deque'"
      order_queue.remove(order_to_cancel)

      if not order_queue:
        del side_book[order_to_cancel.price]  # O(log N)

      del self._orders[order_id]
      return True
    except:
      # Order not found in the dequeue, strong inconsistency
      del self._orders[order_id]
      return False

  def get_best_bid(self) -> Decimal | None:
    """Best bid is the highest bid price, else None"""
    if not self.bids:
      return None

    # self.bids maps ordered price level to orders queue
    return self.bids.peekitem(-1)[0]

  def get_best_ask(self) -> Decimal | None:
    """Best ask is the lowest ask price, else None"""
    if not self.asks:
      return None

    # self.asks maps ordered price level to orders queue
    return self.asks.peekitem(0)[0]

  def get_spread(self) -> Decimal | None:
    best_bid = self.get_best_bid()
    best_ask = self.get_best_ask()

    if best_bid is None or best_ask is None:
      return None

    return best_ask - best_bid

  def get_depth(
    self, levels: int
  ) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
    """Returns aggregated depth of the book up to a certain number of levels."""
    bids_depth = []

    # Iterate bids from highest to lowest
    # self.bids are stored from lowest to highest, so reversed is required
    for price in reversed(self.bids.keys()):
      if len(bids_depth) > levels:
        break
      total_quantity = sum(order.quantity for order in self.bids[price])
      bids_depth.append((price, total_quantity))

    asks_depth = []
    # Iterate asks from lowest to highest
    for price in self.asks.keys():
      if len(asks_depth) > levels:
        break
      total_quantity = sum(order.quantity for order in self.asks[price])
      asks_depth.append((price, total_quantity))

    return bids_depth, asks_depth

  def display_book(self, level: int):
    """Prints formatted representation of the LOB"""
    print("\n------LOB-------")
    bids_depth, asks_depth = self.get_depth(level)
    print("------ ASKS -------")
    if not asks_depth:
      print("No asks")
    else:
      # print from highest to lowest
      # stored from lowest (left) to highest (right)
      for price, quantity in reversed(asks_depth):
        print(f"{price:.3f} : {quantity}")

    print("------ SPREAD -------")
    spread = self.get_spread()
    if spread is None:
      print("Spread: N/A (one sided)")
    else:
      print(f"Spread: {spread}")

    print("------ BIDS-------")
    if not bids_depth:
      print("No bids")
    else:
      # stored from lowest to highest
      for price, quantity in bids_depth:
        print(f"{price:.3f} : {quantity}")

    print("------LOB-------\n")


# Example
if __name__ == "__main__":
  lob = LimitOrderBook()

  # map value to decimal
  dec = lambda val: Decimal(str(val))

  print("Adding example orders")
  o_id1, _ = lob.add_order("BUY", dec(99.5), dec(10))
  o_id2, _ = lob.add_order("BUY", dec(99.8), dec(5))
  o_id3, _ = lob.add_order("BUY", dec(99.5), dec(12))
  o_id4, _ = lob.add_order("SELL", dec(100), dec(20))
  o_id5, _ = lob.add_order("SELL", dec(99.9), dec(10))
  o_id6, _ = lob.add_order("SELL", dec(100), dec(3))

  lob.display_book(level=5)

  # Test single match
  print("Adding new order with cross over...\n")
  o_id7, trades_1 = lob.add_order("SELL", dec(99.7), dec(8))
  print(f"Trades executed by Order: {o_id7}: {trades_1}")
  lob.display_book(level=5)
