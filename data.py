import datetime
import os
import os.path
import pandas as pd

from abc import ABCMeta, abstractmethod

from event import MarketEvent


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested.

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
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


class BitcoinFromCSV(DataHandler):
    """
    BitcoinFromCSV is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.
    """

    def __init__(self, events, csv_path):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.csv_path = csv_path

        self.symbol_data = pd.DataFrame()
        self.latest_symbol_data = pd.DataFrame()
        self.continue_backtest = True

        self._open_convert_csv_files()
        self.length = 0
        self.current_idx = 1
        #self.current_time = datetime.datetime.now()

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

    def _data_streamer(self):
        for row_idx in xrange(len(self.symbol_data)):
            self.current_idx += 1
            yield self.symbol_data.iloc[row_idx]

    def _get_new_bar(self):
        """
        Returns the latest bar from the data feed as a tuple of
        (sybmbol, datetime, open, low, high, close, volume).
        """
        # for row_idx in xrange(len(self.symbol_data)):
        #    yield self.symbol_data.iloc[row_idx:(row_idx+1)]
        # for row in self.symbol_data.iterrows():
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
