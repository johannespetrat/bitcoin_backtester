from data import BitcoinFromCSV
#from strategy import BuyAndHoldStrategy
import Queue
#from signals import MovingAverage
from strategy import TestStrategy
import datetime
from broker import BacktestingBroker
from nose.tools import set_trace
from simulator import Simulator,Order
from portfolio import Portfolio as TestPortfolio
from position import Position

if __name__=="__main__":
    global_event_queue = Queue.Queue()
    dataStream = BitcoinFromCSV(events=global_event_queue, csv_path = '../data/bitcoin_6_months_hourly.csv')
    broker = BacktestingBroker(dataStream=dataStream, event_queue=global_event_queue, commission=0.01)
    testOrder = Order(symbol='BTC',order_type='MKT',exchange='TestExchange',volume=10,
    					side='B',posted_at=datetime.datetime.now())
    fill_event = broker.execute_order(testOrder)
    strategy = TestStrategy(global_event_queue)
    portfolio = TestPortfolio(broker,100)
    simulator = Simulator(dataStream,broker,strategy,portfolio)

    simulator.run()
    while global_event_queue.qsize()>0:
    	try:
    		print global_event_queue.get().price
    	except AttributeError:
    		pass
    set_trace()
