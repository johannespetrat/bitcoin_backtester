import datetime
import numpy as np
import pandas as pd
import Queue

from abc import ABCMeta, abstractmethod

from event import SignalEvent
from simulator import Order


class Strategy(object):
    """
    Strategy is an abstract base class for
    all inherited strategy objects.

    This is designed to work both with historic and live data
    since it obtains quotes from the data stream.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def make_offers(self, bars, signals):
        """

        """
        raise NotImplementedError("Should implement execute_order")


class TestStrategy(Strategy):
    """
    this strategy is used for testing purposes and all it does is continuously
    posting buy market orders
    """

    def __init__(self, events):
        """
        events - The Event Queue object.
        """
        self.events = events
        self.order_id = 0

    def make_offers(self, bars, signals):
        sign = signals.get_signals()
        self.order_id += 1
        #from nose.tools import set_trace; set_trace()
        if bars['Ask']<sign['Moving Average']:
            return [Order(symbol='BTC', order_type='MKT', exchange='TestExchange', volume=10,
                          side='B', posted_at=bars['Datetime'])]
            #return [Order(symbol='BTC', order_type='LMT', exchange='TestExchange', volume=10,
            #              side='B', posted_at=bars['Datetime'] , price=30)]
        elif bars['Bid']>sign['Moving Average']:
            return [Order(symbol='BTC', order_type='MKT', exchange='TestExchange', volume=10,
                          side='S', posted_at=bars['Datetime']) ]
            #return [Order(symbol='BTC', order_type='LMT', exchange='TestExchange', volume=10,
            #              side='S', posted_at=bars['Datetime'], price=30)]
