import unittest
from data import DataHandler
from simulator import Order, Simulator
from broker import BacktestingBroker
import Queue
from nose.tools import set_trace
import datetime 
import pandas as pd
import numpy as np
from event import FillEvent
from portfolio import Portfolio
from signals import SignalCollector

# get coverage of this folder
# nosetests --with-coverage --cover-erase --cover-package=.

class testStream(DataHandler):

    def __init__(self, bars_df):
        self.bars = bars_df
        self.current_bars = pd.DataFrame({'Bid':[], 'Ask':[], 'Datetime': datetime.datetime(1993,02,11)})
        self.current_idx = 0

    def get_latest_bars(self, N=10):
        return self.current_bars.iloc[max(0, self.current_idx-N):self.current_idx]

    def update_bars(self):
        self.current_idx+=1
        self.current_bars = self.bars.iloc[:self.current_idx]

    def _data_streamer(self):
        for bars in self.bars.iterrows():
            self.current_idx +=1
            yield bars[1]


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

    def tearDown(self):
        del self.stream
    
    def compare_events(self, event1, event2):
        if event1 is None and event2 is None:
            pass
        else:
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
            self.compare_events(expectedEvents[idx], event)         
        


class testPortfolio(unittest.TestCase):

    def setUp(self):
        prices = np.array([10,20,30,40])
        self.stream = testStream(pd.DataFrame({'Bid':prices, 'Ask':prices+5}))
        self.stream.update_bars()
        self.event_queue = Queue.Queue()
        self.broker = BacktestingBroker(self.stream, self.event_queue, 0.0)
        self.portfolio = Portfolio(self.broker, 100)

    def tearDown(self):
        del self.event_queue
        del self.stream
        del self.portfolio
        del self.broker

    def test_add_position(self):
        fill_event_buy = FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=21*3, price=21, commission=0.0)                       
        fill_event_sell = FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='S', fill_cost=20*3, price=20, commission=0.0)
        self.portfolio._add_position(fill_event_buy)
        self.portfolio._add_position(fill_event_sell)
        self.assertTrue('BTC' in self.portfolio.positions.keys())
        self.assertEqual(len(self.portfolio.positions.keys()),1)
        self.assertEqual(self.portfolio.positions['BTC'].side, fill_event_buy.side)
        self.assertEqual(self.portfolio.positions['BTC'].exchange, fill_event_buy.exchange)
        self.assertEqual(self.portfolio.positions['BTC'].volume, fill_event_buy.volume)
        market_price = self.stream.get_latest_bars(1).values.mean()

        # have to double-check how pnl is calculated
        expected_unrealised_pnl = fill_event_buy.volume * (market_price - fill_event_buy.price) - (
                                    fill_event_buy.fill_cost)
        #self.assertEqual(self.portfolio.positions['BTC'].unrealised_pnl, )        

    def test_modify_position(self):
        fill_event_buy_1 = FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=21*3, price=21, commission=0.0)
        fill_event_sell = FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=2,
                                    side='S', fill_cost=20*3, price=20, commission=0.0)
        fill_event_buy_2 = FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=21*3, price=21, commission=0.0)
        self.portfolio._add_position(fill_event_buy_1)
        self.portfolio._modify_position(fill_event_sell)        
        self.assertTrue('BTC' in self.portfolio.positions.keys())
        self.assertEqual(len(self.portfolio.positions.keys()),1)
        self.assertEqual(self.portfolio.positions['BTC'].side, fill_event_buy_1.side)
        self.assertEqual(self.portfolio.positions['BTC'].exchange, fill_event_buy_1.exchange)
        self.assertEqual(self.portfolio.positions['BTC'].volume, 
                                fill_event_buy_1.volume - fill_event_sell.volume)        
        self.portfolio._modify_position(fill_event_buy_2)
        self.assertEqual(self.portfolio.positions['BTC'].volume, 
                            fill_event_buy_1.volume + fill_event_buy_2.volume - fill_event_sell.volume)
        # add test for correct pnl

    def test_update(self):
        fill_events = [FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=21*3, price=21, commission=0.0),
                       FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=2,
                                    side='S', fill_cost=20*3, price=20, commission=0.0),
                       FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=21*3, price=21, commission=0.0)]
        self.portfolio.update(fill_events)        
        self.assertEqual(self.portfolio.positions['BTC'].volume, 3 - 2 + 3)
        self.assertTrue('BTC' in self.portfolio.positions.keys())
        self.assertEqual(len(self.portfolio.positions.keys()),1)

    def test_closing_position(self):
        fill_event_buy = FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=21*3, price=21, commission=0.0)
        fill_event_sell = FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='S', fill_cost=20*3, price=20, commission=0.0)
        self.portfolio._add_position(fill_event_buy)
        self.assertEqual(len(self.portfolio.positions.keys()),1)
        self.portfolio._modify_position(fill_event_sell)        
        self.assertEqual(len(self.portfolio.positions.keys()),0)


    def test_buy_to_sell_position(self):
        fill_event_buy = FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=3,
                                    side='B', fill_cost=21*3, price=21, commission=0.0)
        fill_event_sell = FillEvent(timeindex=datetime.datetime.now(), symbol='BTC', 
                                    exchange='TestExchange', volume=4,
                                    side='S', fill_cost=20*4, price=20, commission=0.0)
        self.portfolio.update([fill_event_buy])
        self.assertEqual(self.portfolio.positions['BTC'].side,'B')
        self.assertEqual(self.portfolio.positions['BTC'].volume,3)        
        self.portfolio.update([fill_event_sell])                
        self.assertEqual(self.portfolio.positions['BTC'].side, 'S')
        self.assertEqual(self.portfolio.positions['BTC'].volume,1)
        #set_trace()
        self.assertEqual(self.portfolio.closed_positions[0].side,'B')
        #self.assertEqual(self.portfolio.closed_positions[0].volume,3)


from strategy import Strategy
class test_strategy(Strategy):

    def make_offers(self, bars, signals):
        return [Order(symbol='BTC', order_type='MKT', exchange='TestExchange', volume=10,
                          side='B', posted_at=bars['Datetime'])]
        
class testSimulator(unittest.TestCase):

    def setUp(self):
        prices = np.array([10,20,30,40])
        dates = [datetime.datetime.now()+datetime.timedelta(minutes=i) for i in range(len(prices))]
        self.stream = testStream(pd.DataFrame({'Bid':prices, 'Ask':prices+5, 'Datetime':dates}))        
        self.stream.update_bars()
        self.event_queue = Queue.Queue()
        self.broker = BacktestingBroker(self.stream, self.event_queue, 0.0)
        self.portfolio = Portfolio(self.broker, 100)
        self.signals = SignalCollector({})
        self.strategy = test_strategy()
        self.simulator = Simulator(self.stream, self.broker, self.strategy, self.portfolio, self.signals)

    def tearDown(self):
        del self.stream
        del self.event_queue        
    
    def test_run_historical(self):
        self.simulator.run()
        from nose.tools import set_trace; set_trace()

    def test_run_live(self):
        pass

if __name__ == "__main__":
    test_classes_to_run = [testSignals, testBroker, testPortfolio, testSimulator]

    loader = unittest.TestLoader()

    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    big_suite = unittest.TestSuite(suites_list)

    runner = unittest.TextTestRunner()
    results = runner.run(big_suite)