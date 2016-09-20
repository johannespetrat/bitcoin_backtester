
class Simulator(object):

    def __init__(self, dataStream, broker, strategy, portfolio):
        self.dataStream = dataStream
        self.broker = broker
        self.strategy = strategy
        self.portfolio = portfolio

    def run(self):
        executed_orders = []
        for bar in self.dataStream._data_streamer():
            orders = self.strategy.make_offers(bar)
            executed_orders = []
            if orders:
                for order in orders:
                    executed = self.broker.execute_order(order)
                    executed_orders.append(executed)
            self.portfolio.update(executed_orders)
        return executed_orders


class Order(object):

    def __init__(self, symbol, order_type, exchange, volume, side, posted_at, price=None):
        if order_type != 'MKT':
            self.price = price
        self.symbol = symbol
        self.exchange = exchange
        self.volume = volume
        self.side = side
        self.posted_at = posted_at
        self.order_type = order_type

    def __str__(self):
        if self.order_type == 'MKT':
            return "Symbol {}; {} {} at market price".format(self.symbol, self.side, self.volume)
        else:
            return "Symbol {}; {} {} at price {}".format(self.symbol, self.side, self.volume, self.price)
