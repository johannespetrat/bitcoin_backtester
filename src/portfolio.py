import copy
from position import Position

class Portfolio(object):

    def __init__(self, price_handler, cash):
        """
        When initialised the Portfolio does not contain any
        positions and all values are set to the initial
        cash, with no PnL - realised or unrealised.
        Note that realised_pnl is the running tally pnl from closed
        positions (closed_pnl), as well as realised_pnl
        from currently open positions.
        """
        self.price_handler = price_handler
        self.init_cash = cash
        self.equity = cash
        self.cur_cash = cash
        self.positions = {}
        self.closed_positions = []
        self.realised_pnl = 0
        self.unrealised_pnl = 0

    def _update_portfolio(self):
        """
        Updates the value of all positions that are currently open.
        Value of closed positions is tallied as self.realised_pnl.
        """
        self.unrealised_pnl = 0
        self.equity = self.realised_pnl
        self.equity += self.init_cash
        exchange = "TestExchange"
        for ticker in self.positions:
            pt = self.positions[ticker]
            if self.price_handler.istick():
                market_price = self.price_handler.get_best_bid_ask(
                    ticker, exchange)
                bid = market_price['Bid'].values[0]
                ask = market_price['Ask'].values[0]
            else:
                close_price = self.price_handler.get_last_close(
                    ticker, exchange)
                bid = close_price
                ask = close_price            
            pt.update_market_value(bid, ask)
            self.unrealised_pnl += pt.unrealised_pnl
            pnl_diff = pt.realised_pnl - pt.unrealised_pnl
            self.equity += (
                pt.market_value - pt.cost_basis + pnl_diff
            )

    def _add_position(self, fill_event):
        """
        Adds a new Position object to the Portfolio. This
        requires getting the best bid/ask price from the
        price handler in order to calculate a reasonable
        "market value".
        Once the Position is added, the Portfolio values
        are updated.
        """
        if fill_event.symbol not in self.positions:
            if self.price_handler.istick():
                market_price = self.price_handler.get_best_bid_ask(
                    fill_event.symbol, fill_event.exchange)
                bid = market_price['Bid'].values[0]
                ask = market_price['Ask'].values[0]
            else:
                close_price = self.price_handler.get_last_close(
                    fill_event.symbol, fill_event.exchange)
                bid = close_price
                ask = close_price
            position = Position(symbol=fill_event.symbol, side=fill_event.side,
                                init_volume=fill_event.volume, exchange=fill_event.exchange,
                                init_price=fill_event.price, init_commission=fill_event.commission,
                                bid=bid, ask=ask)
            self.positions[fill_event.symbol] = position
            self._update_portfolio()
        else:
            print(
                "Ticker %s is already in the positions list. "
                "Could not add a new position." % fill_event.symbol
            )

    def _get_closed_and_diff(self, fill_event):
        """ 
        gets called if the volume of a fill event closes the active position
        and converts from buy-side to sell-side (or reversed)
        """        
        diff_fill_event = copy.deepcopy(fill_event)
        closed_fill_event = copy.deepcopy(fill_event)
        closed_volume = self.positions[fill_event.symbol].volume
        diff_volume = fill_event.volume - self.positions[fill_event.symbol].volume
        diff_fill_event.fill_cost = fill_event.fill_cost / float(fill_event.volume) * diff_volume
        closed_fill_event.fill_cost = fill_event.fill_cost / float(fill_event.volume) * closed_volume
        closed_fill_event.volume = closed_volume
        diff_fill_event.volume = diff_volume
        assert(diff_fill_event.volume>0), 'got a negative volume: {}'.format(diff_fill_event.volume)
        assert(closed_fill_event.volume>0), 'got a negative volume: {}'.format(closed_fill_event.volume)        
        return closed_fill_event, diff_fill_event


    def _modify_position(self, fill_event):
        """
        Modifies a current Position object to the Portfolio.
        This requires getting the best bid/ask price from the
        price handler in order to calculate a reasonable
        "market value".
        Once the Position is modified, the Portfolio values
        are updated.
        """             
        exchange = 'TestExchange'
        if fill_event.symbol in self.positions:
            if (self.positions[fill_event.symbol].volume < fill_event.volume) and (
                self.positions[fill_event.symbol].side != fill_event.side):
                fill_event, diff_fill_event = self._get_closed_and_diff(fill_event)
                self.positions[fill_event.symbol].transact_shares(fill_event)
                SURPLUS_FLAG = True                
            else:                
                self.positions[fill_event.symbol].transact_shares(fill_event)
                SURPLUS_FLAG = False
            try:                
                if self.price_handler.istick():
                    market_price = self.price_handler.get_best_bid_ask(
                        fill_event.symbol, exchange)                
                    bid = market_price['Bid'].values[0]
                    ask = market_price['Ask'].values[0]
                else:
                    close_price = self.price_handler.get_last_close(
                        fill_event.symbol, exchange)
                    bid = close_price
                    ask = close_price            
                self.positions[fill_event.symbol].update_market_value(bid, ask)
            
                if self.positions[fill_event.symbol].volume == 0:
                    closed = self.positions.pop(fill_event.symbol)                    
                    self.realised_pnl += closed.realised_pnl
                    self.closed_positions.append(closed)                

                if SURPLUS_FLAG:                    
                    self._add_position(diff_fill_event)
                    self.positions[diff_fill_event.symbol].update_market_value(bid, ask)

                self._update_portfolio()
            except IndexError:
                print("Market Price for ticker %s is not available." % fill_event.symbol)
        else:
            print(
                "Ticker %s not in the current position list. "
                "Could not modify a current position." % fill_event.symbol
            )

    # def transact_position(self, action, ticker,
    #                        volume, price, commission):
    def transact_position(self, fill_event):
        """
        Handles any new position or modification to
        a current position, by calling the respective
        _add_position and _modify_position methods.
        Hence, this single method will be called by the
        PortfolioHandler to update the Portfolio itself.
        """

        if fill_event.side == "B":
            self.cur_cash -= ((fill_event.volume *
                               fill_event.price) + fill_event.commission)
        elif fill_event.side == "S":
            self.cur_cash += ((fill_event.volume *
                               fill_event.price) - fill_event.commission)

        if fill_event.symbol not in self.positions:
            self._add_position(fill_event)
        else:
            self._modify_position(fill_event)

    def update(self, executed_order):
        """
        next thing to do
        """
        for order in executed_order:
            if order is not None:
                self.transact_position(order)
