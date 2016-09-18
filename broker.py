from abc import ABCMeta, abstractmethod
from datetime import datetime
from event import FillEvent

class BasicBroker(object):
    __metaclass__ = ABCMeta
    @abstractmethod
    def execute_order(self, order):
        raise NotImplementedError("Should implement execute_order")
    
    @abstractmethod
    def get_market_price(self, exchange):
        raise NotImplementedError("Should implement get_market_price")


class BacktestingBroker(BasicBroker):
    def __init__(self,dataStream,event_queue,commission):
        self.dataStream = dataStream
        self.event_queue = event_queue
        self.commission = commission                

    def execute_order(self, order):
        market_price = self.get_market_price(order.exchange)
        if order.order_type=='MKT':
            return self._market_order(order, market_price)            
        elif order.order_type=='LMT':
            return self._limit_order(order, market_price)

    def get_market_price(self,exchange):
        data = self.dataStream.get_latest_bars(N=10)
        return data['Weighted Price'].values[-1]

    def _market_order(self,order,market_price):
        volume = order.volume
        fill_cost = volume * market_price       
        fill_event = FillEvent(timeindex = datetime.now(),symbol='BTC',exchange='TestExchange',
                                volume=volume, side=order.side,fill_cost=fill_cost, 
                                commission=volume*self.commission,price=market_price)
        self.event_queue.put(fill_event)
        return fill_event

    def _limit_order(self,order):
        pass
