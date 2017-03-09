from abc import ABCMeta, abstractmethod
from datetime import datetime
from event import FillEvent


class BasicBroker(object):
    """
    This is the base class for the broker. The broker executes orders placed by 
    the trading strategy on the exchange.

    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def execute_order(self, order):
        raise NotImplementedError("Should implement execute_order")

    @abstractmethod
    def get_market_price(self, exchange):
        raise NotImplementedError("Should implement get_market_price")

    def get_best_bid_ask(self, ticker, exchange):
        return self.dataStream.get_latest_bars(N=1)[['Bid','Ask']]

    def get_last_close(self, ticker, exchange):
        return self.dataStream.get_latest_bars(N=1)['Close'].values[0]


class BacktestingBroker(BasicBroker):
    """
    this broker should be used in conjunction with the historical backtester
    """
    def __init__(self, dataStream, event_queue, commission):
        self.dataStream = dataStream
        self.event_queue = event_queue
        self.commission = commission

    def execute_order(self, order):        
        if order.order_type == 'MKT':
            return self._market_order(order)
        elif order.order_type == 'LMT':
            return self._limit_order(order)

    def get_market_price(self, exchange, side):
        data = self.dataStream.get_latest_bars(N=10)
        assert(side=='B' or side=='S'), "side must be 'S' or 'B'"
        if side=='B':
            return data['Ask'].values[-1]
        elif side=='S':
            return data[['Bid']].values[-1]        


    def _market_order(self, order):
        """
        executes a market order
        """
        market_price = self.get_market_price(order.exchange, order.side)
        volume = order.volume
        fill_cost = volume * market_price
        fill_event = FillEvent(timeindex=order.posted_at, symbol='BTC', exchange='TestExchange',
                               volume=volume, side=order.side, fill_cost=fill_cost,
                               commission=volume * self.commission, price=market_price)
        self.event_queue.put(fill_event)
        return fill_event

    def _limit_order(self, order):
        """
        executes a limit order
        """
        volume = order.volume        
        market_price = self.get_market_price(order.exchange, order.side)
        fill_cost = volume * order.price
        if order.side=='B' and order.price>=market_price:
            fill_event = FillEvent(timeindex=order.posted_at, symbol='BTC', exchange='TestExchange',
                                   volume=volume, side=order.side, fill_cost=fill_cost,
                                   commission=volume * self.commission, price=order.price)
            self.event_queue.put(fill_event)
            return fill_event
        elif order.side=='S' and order.price<=market_price:
            fill_event = FillEvent(timeindex=order.posted_at, symbol='BTC', exchange='TestExchange',
                                   volume=volume, side=order.side, fill_cost=fill_cost,
                                   commission=volume * self.commission, price=order.price)
            self.event_queue.put(fill_event)
            return fill_event


    def istick(self):
        return True



class CoinbaseSandboxBroker(BasicBroker):
    def __init__(self, dataStream, event_queue, commission, account_id):
        self.dataStream = dataStream
        self.event_queue = event_queue
        self.commission = commission
        self.client = dataStream.client
        self.account_id = account_id

    def execute_order(self, order):
        if order.side == 'BUY':
            self.client.buy(self.account_id, amount='1', currency='BTC')
        elif order.side == 'SELL':
            self.client.sell(self.account_id, amount='1', currency='BTC')


    def _market_order(self, order, market_price):
        volume = order.volume
        fill_cost = volume * market_price
        fill_event = FillEvent(timeindex=datetime.now(), symbol='BTC', exchange='Coinbase',
                               volume=volume, side=order.side, fill_cost=fill_cost,
                               commission=volume * self.commission, price=market_price)
        self.event_queue.put(fill_event)
        return fill_event

    def _limit_order(self, order):
        pass

    def istick(self):
        return False
