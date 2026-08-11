from schwab_tools.api import APIClient
from schwab_tools.account import Account
from schwab_tools.quote import Quote
from schwab_tools.order import LimitOrder
from schwab_tools.utils import setup_cloud_logging


def main():
  # Setup cloud logging (optional) and APIClient
  setup_cloud_logging()
  client = APIClient()

  # Check out your account
  account = Account(client=client)
  print('My Account Key: ', account.account_key)
  print('My Balance: ', account.check_balance())

  # Get a stock quote
  quote = Quote(client=client, symbol='IBM')
  print(f'Last {quote.symbol} Quote Price: ', quote.get_last_price())

  # Place some orders and stuff
  order1 = LimitOrder(
    client = client,
    account_key = account.account_key,
    symbol = 'NVDA',
    action = 'BUY',
    quantity = 1,
    price = 50.00)
  order1.place_order()
  order1.run_when_status(
    'CANCELLED', 
    func = print, 
    func_args = ['Test message'])
  
  order2 = LimitOrder(
    client = client,
    account_key = account.account_key,
    symbol = 'NFLX',
    action = 'BUY',
    quantity = 1,
    price = 50.00)
  order2.place_order()
  order2.run_when_status(
    'CANCELLED',
    order1.cancel_order)
  
  order2.cancel_order()


if __name__ == '__main__':
  main()