import time
import pytest
from collections import deque
from decimal import Decimal

from lob import LimitOrderBook, Order


# Helper for convertion to Decimal
dec = lambda x: Decimal(str(x))


@pytest.fixture
def lob():
  """Fresh instance of LimitOrderBook per test"""
  return LimitOrderBook()


def test_initial_state(lob):
  assert len(lob.bids) == 0
  assert len(lob.asks) == 0
  assert len(lob._orders) == 0
  assert lob.get_best_ask() is None
  assert lob.get_best_bid() is None


def test_add_single_buy_order(lob):
  order_id, trades = lob.add_order("BUY", dec(100), dec(10))
  assert len(lob.trades) == 0
  assert len(trades) == 0
  assert order_id is not None
  assert len(lob.bids) == 1
  assert len(lob.asks) == 0
  assert len(lob._orders) == 1
  assert isinstance(lob.bids[dec(100)], deque)
  assert isinstance(lob.bids[dec(100)][0], Order)
  assert lob.bids[dec(100)][0].order_id == order_id
  assert lob.bids[dec(100)][0].quantity == dec(10)

def test_add_single_sell_order(lob):
  order_id, trades = lob.add_order("SELL", dec(105), dec(5))
  assert len(lob.trades) == 0
  assert len(trades) == 0
  assert order_id is not None
  assert len(lob.bids) == 0
  assert len(lob.asks) == 1
  assert len(lob._orders) == 1
  assert isinstance(lob.asks[dec(105)], deque)
  assert isinstance(lob.asks[dec(105)][0], Order)
  assert lob.asks[dec(105)][0].order_id == order_id
  assert lob.asks[dec(105)][0].quantity == dec(5)


def test_add_multiple_orders_no_match(lob):
    oid1, _ = lob.add_order('BUY', dec(99), dec(10))
    oid2, _ = lob.add_order('BUY', dec(98), dec(5))
    oid3, _ = lob.add_order('SELL', dec(101), dec(15))
    oid4, _ = lob.add_order('SELL', dec(102), dec(20))

    assert len(lob.bids) == 2
    assert len(lob.asks) == 2
    assert len(lob._orders) == 4
    assert lob.get_best_bid() == dec(99)
    assert lob.get_best_ask() == dec(101)
    assert lob.get_spread() == dec(2)


def test_fifo_at_same_price_level(lob):
  oid1, _ = lob.add_order("BUY", dec(100), dec(10))
  oid2, _ = lob.add_order("BUY", dec(100), dec(14))

  assert len(lob.bids[dec(100)]) == 2
  assert lob.bids[dec(100)][0].order_id == oid1
  assert lob.bids[dec(100)][1].order_id == oid2
  
  # Match with different order
  _, trades = lob.add_order("SELL", dec(100), dec(12))
  assert len(lob.trades) == 2

  # Full fill first order
  assert trades[0]["maker_order_id"] == oid1
  assert trades[0]["quantity"] == dec(10)
  assert trades[0]["price"] == dec(100)

  # Partial fill second order
  assert trades[1]["maker_order_id"] == oid2
  assert trades[1]["quantity"] == dec(2)
  assert trades[1]["price"] == dec(100)

  # Remaining order is partial oid2
  assert len(lob.bids[dec(100)]) == 1
  assert lob.bids[dec(100)][0].order_id == oid2
  assert lob.bids[dec(100)][0].quantity == dec(12)
  assert oid2 in lob._orders
  assert oid1 not in lob._orders


def test_cancel_existing_sell_order(lob):
    oid1, _ = lob.add_order('SELL', dec(101), dec(10))
    oid2, _ = lob.add_order('SELL', dec(101), dec(5))
    oid3, _ = lob.add_order('SELL', dec(102), dec(15))

    assert len(lob._orders) == 3
    assert len(lob.asks) == 2
    assert len(lob.asks[dec(101)]) == 2

    cancelled = lob.cancel_order(oid2)
    assert cancelled

    assert len(lob._orders) == 2
    assert oid2 not in lob._orders
    assert oid1 in lob._orders
    assert oid3 in lob._orders

    assert len(lob.asks) == 2
    assert len(lob.asks[dec(101)]) == 1
    assert lob.asks[dec(101)][0].order_id == oid1


def test_cancel_non_existent_order(lob):
    oid1, _ = lob.add_order('BUY', dec(99), dec(10))
    cancelled = lob.cancel_order(999) # ID doesn't exist
    assert not cancelled

    assert len(lob._orders) == 1
    assert len(lob.bids) == 1


@pytest.mark.parametrize(
      "setup_bids, setup_asks, expected_spread", [
        ([], [], None),
        ([("BUY", dec(99), dec(10))], [], None),
        ([], [("SELL", dec(99), dec(10))], None),
        ([("BUY", dec(99.9), dec(10))], [("SELL", dec(101.1), dec(2))], dec(1.2))
      ]
)
def test_spread_computation(
      lob, setup_bids, setup_asks, expected_spread
):
  for side, price, quantity in setup_bids:
      lob.add_order(side, price, quantity)
  for side, price, quantity in setup_asks:
      lob.add_order(side, price, quantity)

  assert lob.get_spread() == expected_spread



@pytest.mark.parametrize("side, price, quantity, expected_exception", [
    ('INVALID', dec(100), dec(10), ValueError),
    ('SELL', dec(-10), dec(10), ValueError),
    ('SELL', dec(100), dec(-5), ValueError),
])
def test_invalid_order_inputs(lob, side, price, quantity, expected_exception):
    with pytest.raises(expected_exception):
        lob.add_order(side, price, quantity)
