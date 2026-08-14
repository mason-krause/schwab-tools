import time
import datetime
import json
import asyncio
from contextlib import suppress
# from asgiref.sync import sync_to_async
from schwab.streaming import UnexpectedResponseCode
from schwab.client.base import BaseClient
from schwab_tools.api import APIClient
from schwab_tools.utils import log_in_background, start_thread 
import pprint

class Account:
  '''
  :param APIClient client: your :ref:`APIClient <api_client>`
  :param str account_key: (optional) manually specify your account key 
  '''
  def __init__(self, client:APIClient, account_key='', background_monitor=False):
    self.client = client
    self.account_key = account_key if account_key else self.list_accounts()[0]['hashValue']
    self.monitoring_active = False
    self.subscribed_orders = {}
    if background_monitor == True:
      self.monitor_in_background()

  def list_accounts(self):
    '''
    Provides details on all brokerage accounts connected to the active API user session
    '''
    response, status_code = self.client.get_account_numbers(parsed_response=True)
    if status_code == 200:
      return response
    else:
      log_in_background(
        called_from = 'list_accounts',
        tags = ['user-message'], 
        message = time.strftime('%H:%M:%S', time.localtime()) + ': Error getting account list, retrying',
        account_key = self.account_key)
      return self.list_accounts()
    
  def view_accounts(self):
    '''
    Provides details on all brokerage accounts connected to the active API user session
    '''
    response, status_code = self.client.get_accounts(parsed_response=True)
    if status_code == 200:
      return response
    else:
      log_in_background(
        called_from = 'view_accounts',
        tags = ['user-message'], 
        message = time.strftime('%H:%M:%S', time.localtime()) + ': Error getting account details, retrying',
        account_key = self.account_key)
      return self.view_accounts()

  def check_balance(self):
    '''
    Returns the cash balance of your account
    '''
    response, status_code = self.client.get_account(parsed_response=True, account_hash=self.account_key)
    try:
      balance = response['securitiesAccount']['currentBalances']['cashBalance']
      return balance
    except Exception as e:
      log_in_background(
        called_from = 'check_balance',
        tags = ['user-message'], 
        message = time.strftime('%H:%M:%S', time.localtime()) + ': Error getting account balance, retrying',
        e = e,
        account_key = self.account_key)
      return self.check_balance()
    
  def check_long_market_value(self):
    '''
    Returns the aggregate value of all securities for long positions in the account
    '''
    response, status_code = self.client.get_account(parsed_response=True, account_hash=self.account_key)
    try:
      balance = response['securitiesAccount']['currentBalances']['longMarketValue']
      return balance
    except Exception as e:
      log_in_background(
        called_from = 'check_long_market_value',
        tags = ['user-message'], 
        message = time.strftime('%H:%M:%S', time.localtime()) + ': Error getting long market value, retrying',
        e = e,
        account_key = self.account_key)
      return self.check_long_market_value()
    
  def view_portfolio(self):
    '''
    Provides details for your account portfolio
    '''
    response, status_code = self.client.get_account(parsed_response=True, account_hash=self.account_key, fields=BaseClient.Account.Fields.POSITIONS)
    try:
      positions = response['securitiesAccount']['positions']
      return positions
    except Exception as e:
      log_in_background(
        called_from = 'Account.view_portfolio',
        tags = ['user-message'], 
        message = time.strftime('%H:%M:%S', time.localtime()) + ': Error getting portfolio info, retrying',
        e = e,
        account_key = self.account_key)
      return self.view_portfolio()    
    
  def get_order_history(self, start_datetime=None, end_datetime=None):
    '''
    Provides details for all orders placed in account over specified time range
    '''
    all_orders = []
    end_datetime = end_datetime or datetime.datetime.now(datetime.timezone.utc)
    while True:
      response, status_code = self.client.get_orders_for_account(parsed_response=True, account_hash=self.account_key, from_entered_datetime=start_datetime, to_entered_datetime=end_datetime)
      if status_code != 200:
        log_in_background(
          called_from = 'Account.get_order_history',
          tags = ['user-message'], 
          message = time.strftime('%H:%M:%S', time.localtime()) + ': Error getting account order history',
          account_key = self.account_key)
        break
      if not response:
        break
      all_orders.extend(response)
      earliest_result_dt = datetime.datetime.strptime(response[-1]['enteredTime'], '%Y-%m-%dT%H:%M:%S%z')
      if earliest_result_dt >= end_datetime:
        break
      end_datetime = earliest_result_dt
    return all_orders

  def get_transaction_history(self, start_datetime=None, end_datetime=None):
    '''
    Provides details for all transactions over specified time range
    '''
    response, status_code = self.client.get_transactions(parsed_response=True, account_hash=self.account_key, start_date=start_datetime, end_date=end_datetime)
    if status_code == 200:
      return response
    else:
      log_in_background(
        called_from = 'Account.get_transaction_history',
        tags = ['user-message'], 
        message = time.strftime('%H:%M:%S', time.localtime()) + ': Error getting account transaction history',
        account_key = self.account_key)

  def lookup_exited_positions(self, trailing_days=31): # maybe want to restrict to exitied with a loss # maybe we should even track the agragate loss for each ticker
    now = datetime.datetime.now(datetime.timezone.utc)
    start_date = now - datetime.timedelta(days=trailing_days)
    exited_symbols = set()
    history = self.get_order_history(start_datetime=start_date, end_datetime=now) 
    for order in history:
      for leg in order.get('orderLegCollection', []):
        if leg.get('positionEffect') == 'CLOSING':
          exited_symbols.add(leg['instrument']['symbol'])
    return list(exited_symbols) 

  async def _stream_account_updates(self):
    try:
      await self.client.login() # should we move this to APIClient.__init__?
    except UnexpectedResponseCode as e:
      # print(e)
      # print('unexpected response, should we retry login?')
      # await sync_to_async(self.client.session.login)(new_token=False)
      await asyncio.get_event_loop().run_in_executor(None, self.client.session.login, False)
      return await self._stream_account_updates()
    self.client.add_account_activity_handler(handler=self.account_message_handler)
    await self.client.account_activity_sub()
    while self.monitoring_active == True:
      await self.client.handle_message()
    await self.client.account_activity_unsubs()   
    await self.client.logout()

  def _monitor_account_updates(self):
    if self.monitoring_active == False:
      self.monitoring_active = True
      loop = asyncio.new_event_loop()
      asyncio.set_event_loop(loop)
      loop.run_until_complete(self._stream_account_updates())
      loop.close()
      self.monitoring_active = False

  def monitor_in_background(self):
    '''
    Monitors account updates using asyncio
    '''
    start_thread(self._monitor_account_updates)

  def account_message_handler(self, message):
    # print(json.dumps(message, indent=2)) ##
    updates = message.get('content', [])
    if updates != []:
      updated_orders = set()
      for update in updates:
        if 'MESSAGE_TYPE' in update:
          update_type = update.get('MESSAGE_TYPE', '')
          update_details = {} if update.get('MESSAGE_DATA', '') == '' else json.loads(update['MESSAGE_DATA'])
        else: # might not need this if api updates are permanent
          update_type = update.get('FIELD_2', '')
          update_details = {} if update.get('FIELD_3', '') == '' else json.loads(update['FIELD_3'])
        # print(json.dumps(update_details, indent=2)) ##
        order_id = update_details.get('SchwabOrderID', '')
        update_msg = ''
        with suppress(Exception):
          update_msg = update_details['BaseEvent']['OrderUROutCompletedEvent']['ValidationDetail'][0]['NgOMSRuleDescription']
        if order_id != '':
          updated_orders.add(order_id)
        if update_msg != '':
          log_in_background(
            called_from = 'account_message_handler', 
            tags = ['user-message'], 
            message = '{}: Order Update- {} (Order # {})'.format(
              time.strftime('%H:%M:%S', time.localtime()),
              update_msg,
              order_id),
            account_key = self.account_key)
      for id in updated_orders:
        if id in self.subscribed_orders:
          self.subscribed_orders[id].check_status()

  def add_order_subscription(self, order):
    self.subscribed_orders[order.order_id] = order
    self.monitor_in_background()

  def remove_order_subscription(self, order_id, deactivate_monitoring=False):
    if order_id in self.subscribed_orders:
      del self.subscribed_orders[order_id]
    if len(self.subscribed_orders) == 0 and deactivate_monitoring == True:
      self.monitoring_active = False