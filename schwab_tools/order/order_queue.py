import time
from schwab_tools.utils import start_thread, log_in_background


class OrderQueue:
  '''
  A class for managing queues of orders

  '''
  def __init__(self,  orders=[]):
    self.orders = orders
    self.placed_order_count = 0

  def add_order(self, order):
    self.orders.append(order)

  def add_orders(self, orders):
    for order in orders:
      self.orders.append(order)

  def mark_complete(self):
    self.placed_order_count += 1
    if self.placed_order_count == len(self.orders):
      log_in_background(
        called_from = 'OrderQueue.mark_complete',
        tags = ['user-message'], 
        message = time.strftime('%H:%M:%S', time.localtime()) + ': Last item completed, printing summary',
        account_key = self.orders[0].account.account_key)
      self.print_summary()
  
  def print_summary(self):
    total_cost = 0.0
    incomplete_order_count = 0
    for order in self.orders:
      total_cost += order.price * order.quantity
      if order.price == 0.0:
        incomplete_order_count += 1
    log_in_background(
      called_from = 'OrderQueue.print_summary',
      tags = ['user-message'], 
      message = time.strftime('%H:%M:%S', time.localtime()) + f': Placed {str(len(self.orders))} orders totaling {total_cost}; Total Failed: {incomplete_order_count}',
      account_key = self.orders[0].account.account_key)

  def place_all(self):
    log_in_background(
        called_from = 'OrderQueue.place_all',
        tags = ['user-message'], 
        message = time.strftime('%H:%M:%S', time.localtime()) + f' Placing {str(len(self.orders))} orders',
        account_key = self.orders[0].account.account_key)
    for idx, order in enumerate(self.orders, start=1):
      start_thread(order.place_and_update, kwargs={'func': self.mark_complete})
      if idx == len(self.orders): # last item
        log_in_background(
        called_from = 'OrderQueue.place_all',
        tags = ['user-message'], 
        message = time.strftime('%H:%M:%S', time.localtime()) + f' Starting last item ({order.symbol}, idx: {idx}, count : {len(self.orders)})',
        account_key = self.orders[0].account.account_key)
      time.sleep(.3)