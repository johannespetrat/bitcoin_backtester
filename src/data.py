import datetime
import time
import os
import os.path
import pandas as pd

from abc import ABCMeta, abstractmethod

from event import MarketEvent

class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all data handlers (both live and historic).

    This will replicate how a live interface with an exchange would provide
    data to our trading strategies.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")

    @abstractmethod
    def _data_streamer(self):
        """
        streams bars
        """
        raise NotImplementedError("Should implement _data_streamer()")


class BitcoinFromCSV(DataHandler):
    """
    Reads historical BTC prices from a csv file
    """

    def __init__(self, events, csv_path, spread):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and an event queue.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.csv_path = csv_path
        self.spread = spread

        self.symbol_data = pd.DataFrame()
        self.latest_symbol_data = pd.DataFrame()
        self.continue_backtest = True

        self._open_convert_csv_files()
        self.length = 0
        self.current_idx = 1        

    def _open_convert_csv_files(self):
        """
        Opens the CSV file from the data directory, converting
        it into pandas DataFrames within a symbol dictionary.

        """
        symbol_data = pd.read_csv(self.csv_path)
        symbol_data['Datetime'] = symbol_data[
            'Timestamp'].apply(lambda x: pd.to_datetime(x))
        symbol_data = symbol_data.sort_values(by='Datetime')
        self.symbol_data = symbol_data
        #self.current_time = symbol_data['Datetime'].min()
        self.latest_symbol_data = pd.DataFrame(
            dict([(c, {}) for c in self.symbol_data.columns]))
        self.symbol_data['Weighted Price'] = pd.to_numeric(
            self.symbol_data['Weighted Price'], errors='coerce')
        self.symbol_data['Bid'] = self.symbol_data['Weighted Price'] - self.spread
        self.symbol_data['Ask'] = self.symbol_data['Weighted Price'] + self.spread
        print self.symbol_data.columns
        self.symbol_data = self.symbol_data[['Datetime','Bid','Ask']]

    def _data_streamer(self):
        for row_idx in xrange(len(self.symbol_data)):
            self.current_idx += 1
            yield self.symbol_data.iloc[row_idx]

    def _get_new_bar(self):
        """
        Returns the latest bar from the data feed as a tuple of
        (sybmbol, datetime, open, low, high, close, volume).
        """
        yield self.symbol_data.iloc[self.length:(self.length + 1)]

    def get_latest_bars(self, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        return self.symbol_data.iloc[max([self.current_idx - N, 0]):self.current_idx]

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        try:
            bar = self._get_new_bar().next()
        except StopIteration:
            self.continue_backtest = False
        else:
            if bar is not None:
                self.latest_symbol_data = pd.concat(
                    [self.latest_symbol_data, bar])
        self.length += 1
        self.events.put(MarketEvent())



class CoinbaseSandboxStream(DataHandler):
    def __init__(self, events, update_rate, client):
        """
        Uses the Coinbase sandbox API to livestream BTC prices and place orders.

        Parameters:
        events - The Event Queue.
        update_rate - The frequency of API calls
        client - An instance of the coinbase client
        """
        self.events = events
        self.update_rate = update_rate

        self.symbol_data = pd.DataFrame()
        self.latest_symbol_data = pd.DataFrame()
        self.continue_backtest = True
        self.client = client

    def _api_listener(self):
        while True:
            time.sleep(self.update_rate)
            buy_price = self.client.get_buy_price(currency_pair = 'BTC-USD')
            sell_price = self.client.get_sell_price(currency_pair = 'BTC-USD')
            date_time = datetime.datetime.strptime(self.client.get_time()['iso'],"%Y-%m-%dT%H:%M:%SZ")
            yield pd.DataFrame({'buy':buy_price, "sell":sell_price, "datetime":date_time})

    def _data_streamer(self):
        while True:
            time.sleep(self.update_rate)
            buy_price = self.client.get_buy_price(currency_pair = 'BTC-USD')
            sell_price = self.client.get_sell_price(currency_pair = 'BTC-USD')
            date_time = datetime.datetime.strptime(self.client.get_time()['iso'],"%Y-%m-%dT%H:%M:%SZ")
            yield pd.DataFrame({"datetime": [date_time], 'buy':[buy_price['amount']], "sell": [sell_price['amount']]})

    def get_latest_bars(self, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        return self.symbol_data.iloc[max([self.current_idx - N, 0]):self.current_idx]

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        try:
            bar = self._get_new_bar().next()
        except StopIteration:
            self.continue_backtest = False
        else:
            if bar is not None:
                self.latest_symbol_data = pd.concat(
                    [self.latest_symbol_data, bar])
        self.length += 1
        self.events.put(MarketEvent())