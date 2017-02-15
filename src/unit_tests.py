import unittest
from data import DataHandler
from simulator import Order
from broker import BacktestingBroker
import Queue
from nose.tools import set_trace
import datetime 
import pandas as pd
import numpy as np
from event import FillEvent

class testStream(DataHandler):

    def __init__(self, bars_df):
        self.bars = bars_df
        self.current_bars = pd.DataFrame({'Bid':[], 'Ask':[]})
        self.current_idx = 0

    def get_latest_bars(self, N=10):
        return self.current_bars.iloc[max(0, self.current_idx-N):self.current_idx]

    def update_bars(self):
        self.current_idx+=1
        self.current_bars = self.bars.iloc[:self.current_idx]

    def _data_streamer(self):
        for bars in self.bars_list.iterrows():
            self.current_idx +=1
            yield bars


class testSignals(unittest.TestCase):

    @unittest.skip("testing skipping")
    def test_skip_me(self):
        self.fail("shouldn't happen")

    def test_normal(self):
        self.assertEqual(1, 1)


class testBroker(unittest.TestCase):

    def setUp(self):
        #self.stream = testStream([{'Bid':p, 'Ask':p+5} for p in [10,20,30,40]])
        prices = np.array([10,20,30,40])
        self.stream = testStream(pd.DataFrame({'Bid':prices, 'Ask':prices+5}))
        print('setting up')

    def tearDown(self):
        print('tearing down')
    
    def compare_events(self, event1, event2):
        self.assertEqual(event1.symbol, event2.symbol)
        self.assertEqual(event1.exchange, event2.exchange)
        self.assertEqual(event1.volume, event2.volume)
        self.assertEqual(event1.side, event2.side)
        self.assertEqual(event1.fill_cost, event2.fill_cost)
        self.assertEqual(event1.price, event2.price)
        self.assertEqual(event1.commission, event2.commission)            

    # test market and limit order buy sell and in-and out of the money
    def test_market_orders(self):
        event_queue = Queue.Queue()
        broker = BacktestingBroker(self.stream, event_queue, 0.0)
        self.stream.update_bars()
        self.stream.update_bars()
        self.stream.update_bars()        
        orders = [Order('BTC', order_type='MKT', exchange='TestExchange', volume=3, side='B', posted_at=datetime.datetime.now(), price=None),
                  Order('BTC', order_type='MKT', exchange='TestExchange', volume=3, side='S', posted_at=datetime.datetime.now(), price=None)]
        expectedEvents = [FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=35*3, price=35, commission=0.0),
                          FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='S', fill_cost=30*3, price=30, commission=0.0)]    

        for idx, order in enumerate(orders):
            event = broker.execute_order(order)
            expectedEvents[idx].timeindex = event.timeindex             
            self.compare_events(expectedEvents[idx], event)         

    def test_limit_orders(self):
        event_queue = Queue.Queue()
        broker = BacktestingBroker(self.stream, event_queue, 0.0)
        self.stream.update_bars()
        self.stream.update_bars()
        self.stream.update_bars()        
        orders = [Order('BTC', order_type='LMT', exchange='TestExchange', volume=3, side='B', 
                        posted_at=datetime.datetime.now(), price=45),
                  Order('BTC', order_type='LMT', exchange='TestExchange', volume=3, side='B', 
                        posted_at=datetime.datetime.now(), price=35),
                  Order('BTC', order_type='LMT', exchange='TestExchange', volume=3, side='B', 
                        posted_at=datetime.datetime.now(), price=25),
                  Order('BTC', order_type='LMT', exchange='TestExchange', volume=3, side='S', 
                        posted_at=datetime.datetime.now(), price=45),
                  Order('BTC', order_type='LMT', exchange='TestExchange', volume=3, side='S', 
                        posted_at=datetime.datetime.now(), price=35),
                  Order('BTC', order_type='LMT', exchange='TestExchange', volume=3, side='S', 
                        posted_at=datetime.datetime.now(), price=25)]
        
        expectedEvents = [FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=45*3, price=45, commission=0.0),
                          FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=35*3, price=35, commission=0.0),
                          None,
                          None,
                          None,
                          FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='S', fill_cost=25*3, price=25, commission=0.0)]    

        for idx, order in enumerate(orders):
            event = broker.execute_order(order)
            expectedEvents[idx].timeindex = event.timeindex             
            self.compare_events(expectedEvents[idx], event)         
        


class testPortfolio(unittest.TestCase):

    @unittest.skip("testing skipping")
    def test_skip_me(self):
        self.fail("shouldn't happen")


if __name__ == "__main__":
    test_classes_to_run = [testSignals, testBroker, testPortfolio]

    loader = unittest.TestLoader()

    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    big_suite = unittest.TestSuite(suites_list)

    runner = unittest.TextTestRunner()
    results = runner.run(big_suite)