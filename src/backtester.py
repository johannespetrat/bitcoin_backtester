from data import BitcoinFromCSV, CoinbaseSandboxStream
#from strategy import BuyAndHoldStrategy
import Queue
#from signals import MovingAverage
from strategy import TestStrategy
import datetime
from broker import BacktestingBroker, CoinbaseSandboxBroker
from nose.tools import set_trace
from simulator import Simulator, Order
from portfolio import Portfolio as TestPortfolio
from position import Position
from signals import SignalCollector, MovingAverage

from coinbase.wallet.client import Client

if __name__ == "__main__":
    global_event_queue = Queue.Queue()
    """
    coinbase_client = Client(api_key = "FE0WouRatrXjTKCB", 
                                api_secret = "d0WwSqfvXmo4DlSiDkhGnIsdsQG6DocK",
                                base_api_uri='https://api.sandbox.coinbase.com/')
    account_id = coinbase_client.get_accounts()['data'][0]['id']
    dataStream = CoinbaseSandboxStream(global_event_queue, update_rate = 1, client = coinbase_client)

    broker = CoinbaseSandboxBroker(dataStream, global_event_queue, commission=0.01, account_id=account_id)
    a = coinbase_client.buy(account_id, amount='1', currency='BTC')

    #for i in d._data_streamer():
    #    print i
    """
    
    dataStream = BitcoinFromCSV(
                            events=global_event_queue, 
                            csv_path='../data/bitcoin_6_months_hourly.csv',
                            spread = 0.3)
    broker = BacktestingBroker(
        dataStream=dataStream, event_queue=global_event_queue, commission=0.01)
    testOrder = Order(symbol='BTC', order_type='MKT', exchange='TestExchange', volume=10,
                      side='B', posted_at=datetime.datetime.now())
    fill_event = broker.execute_order(testOrder)
    strategy = TestStrategy(global_event_queue)
    portfolio = TestPortfolio(broker, 100)
    signals = SignalCollector({"Moving Average": MovingAverage(lookback_period = datetime.timedelta(hours=6))})
    simulator = Simulator(dataStream, broker, strategy, portfolio, signals)

    simulator.run()
    while global_event_queue.qsize() > 0:
        try:
            event = global_event_queue.get()
            print type(event), event.price, event.side
        except AttributeError:
            pass
    set_trace()