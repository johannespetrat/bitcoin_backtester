import cPickle as pickle

class Simulator(object):

    def __init__(self, dataStream, broker, strategy, portfolio, signals):
        self.dataStream = dataStream
        self.broker = broker
        self.strategy = strategy
        self.portfolio = portfolio
        self.signals = signals
        self.executed_orders = []
        self.pnls = {}

    def run(self):
        all_executed_orders = []
        for bar in self.dataStream._data_streamer():           
            print bar['Datetime'] 
            #if bar['Datetime'].month<8 and bar['Datetime'].month>1:
            #    break
            self.signals.update(bar)
            orders = self.strategy.make_offers(bar, self.signals)
            executed_orders = []
            if orders:
                for order in orders:
                    executed = self.broker.execute_order(order)                    
                    print executed
                    executed_orders.append(executed)
                    all_executed_orders.append(executed)
            self.portfolio.update(executed_orders)
            #from nose.tools import set_trace; set_trace()
            self.pnls[bar['Datetime']] = {'realised': self.portfolio.realised_pnl,
                                          'unrealised': self.portfolio.unrealised_pnl}
        self.executed_orders = all_executed_orders
        return executed_orders

    def save_results(self, filename):
        results = {
                     "Trades": self.executed_orders,
                     "Bid": self.dataStream.symbol_data['Bid'].values,
                     "Ask": self.dataStream.symbol_data['Ask'].values,
                     "Datetime": self.dataStream.symbol_data['Datetime'].values,
                     "Realised_PnL": [pnl['realised'] for pnl in self.pnls.values()],
                     "Unrealised_PnL": [pnl['unrealised'] for pnl in self.pnls.values()]
                     }
        with open(filename,'w') as fp:
            pickle.dump(results,fp)

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
