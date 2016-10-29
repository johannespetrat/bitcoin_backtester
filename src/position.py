class Position(object):

    def __init__(self, symbol, side, init_volume, exchange,
                 init_price, init_commission, bid, ask):
        """
        Set up the initial "account" of the Position to be
        zero for most items, with the exception of the initial
        purchase/sale.

        Then calculate the initial values and finally update the
        market value of the transaction.
        """
        self.side = side
        self.symbol = symbol
        self.volume = init_volume
        self.init_price = init_price
        self.init_commission = init_commission
        self.exchange = exchange

        self.realised_pnl = 0
        self.unrealised_pnl = 0

        self.buys = 0
        self.sells = 0
        self.avg_bot = 0
        self.avg_sld = 0
        self.total_bot = 0
        self.total_sld = 0
        self.total_commission = init_commission

        self._calculate_initial_value()
        self.update_market_value(bid, ask)

    def _calculate_initial_value(self):
        """
        Depending upon whether the action the position is long or short
        calculate the average bought cost, the total bought
        cost, the average price and the cost basis.

        Then calculate the net total with and without commission.
        """

        if self.side == "B":
            self.buys = self.volume
            self.avg_bot = self.init_price
            self.total_bot = self.buys * self.avg_bot
            self.avg_price = (self.init_price * self.volume +
                              self.init_commission) // self.volume
            self.cost_basis = self.volume * self.avg_price
        else:  # side=='S'
            self.sells = self.volume
            self.avg_sld = self.init_price
            self.total_sld = self.sells * self.avg_sld
            self.avg_price = (self.init_price * self.volume -
                              self.init_commission) // self.volume
            self.cost_basis = -self.volume * self.avg_price
        self.net = self.buys - self.sells
        self.net_total = self.total_sld - self.total_bot
        self.net_incl_comm = self.net_total - self.init_commission

    def update_market_value(self, bid, ask):
        """
        The market value is tricky to calculate as we only have
        access to the top of the order book through Interactive
        Brokers, which means that the true redemption price is
        unknown until executed.

        However, it can be estimated via the mid-price of the
        bid-ask spread. Once the market value is calculated it
        allows calculation of the unrealised and realised profit
        and loss of any transactions.
        """
        midpoint = (bid + ask) // 2
        self.market_value = self.volume * midpoint
        self.unrealised_pnl = self.market_value - self.cost_basis
        self.realised_pnl = self.market_value + self.net_incl_comm

    def transact_shares(self, fill_event):
        """
        Calculates the adjustments to the Position that occur
        once new shares are bought and sold.

        Takes care to update the average bought/sold, total
        bought/sold, the cost basis and PnL calculations,
        as carried out through Interactive Brokers TWS.
        """
        self.total_commission += fill_event.commission

        # Adjust total bought and sold
        if fill_event.side == "B":
            self.avg_bot = (self.avg_bot * self.buys + fill_event.price * fill_event.volume) \
                // (self.buys + fill_event.volume)
            if self.side != "S":
                self.avg_price = (self.avg_price * self.buys +
                                  fill_event.price * fill_event.volume + fill_event.commission)\
                    // (self.buys + fill_event.volume)
            self.buys += fill_event.volume
            self.total_bot = self.buys * self.avg_bot

        # action == "SLD"
        else:
            self.avg_sld = (self.avg_sld * self.sells + fill_event.price * fill_event.volume) \
                // (self.sells + fill_event.volume)
            if self.side != "B":
                self.avg_price = (self.avg_price * self.sells +
                                  fill_event.price * fill_event.volume - fill_event.commission) \
                    // (self.sells + fill_event.volume)
            self.sells += fill_event.volume
            self.total_sld = self.sells * self.avg_sld

        # Adjust net values, including commissions
        self.net = self.buys - self.sells
        self.volume = self.net
        self.net_total = self.total_sld - self.total_bot
        self.net_incl_comm = self.net_total - self.total_commission

        # Adjust average price and cost basis
        self.cost_basis = self.volume * self.avg_price
