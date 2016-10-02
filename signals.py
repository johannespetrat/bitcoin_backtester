import datetime
import os
import os.path
import pandas as pd

from abc import ABCMeta, abstractmethod

from event import MarketEvent

import warnings

import numpy as np

from event import SignalEvent
from datetime import datetime


class SignalCollector(object):
    def __init__(self, signals):
        self.signals = signals        

    def update(self,bars):
        for signal in self.signals.values():
            signal.update(bars)

    def get_signals(self):
        signal_values = dict([signal_name,None] for signal_name in self.signals.keys())
        for signal in self.signals.values():  
            tmp_dict = signal.get_values()
            signal_values[tmp_dict.keys()[0]] = tmp_dict.values()[0]
        return signal_values


class SignalGenerator(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        """
        takes a data stream and generates signals
        """
        pass

    def update(self, bar):
        raise NotImplementedError("update() not implemented")

    def get_values(self):
        raise NotImplementedError("get_values() not implemented")        


class MovingAverage(SignalGenerator):

    def __init__(self, lookback_period):
        """
        takes a data stream and generates signals
        """
        self.lookback_period = lookback_period
        self.data = pd.DataFrame({"Datetime":[],"Price":[]})
        self.ma = None

    def update(self, bar):
        self.data = self.data.append(pd.DataFrame({"Datetime":[bar['Datetime']],"Price":[bar['Weighted Price']]}))        
        lookback_datetime = bar['Datetime'].replace(minute=0,second=0) - self.lookback_period
        self.data = self.data[self.data['Datetime']>=lookback_datetime]
        #from nose.tools import set_trace; set_trace()
        if len(self.data) < self.lookback_period.seconds/3600.:
            warnings.warn("not enough data points available")
            self.ma = self.data.groupby('Datetime').min()['Price'] + self.data.groupby('Datetime').max()['Price']
            #return SignalEvent(symbol='BTC', datetime=datetime.now(), signal_type='Moving Average')
        else:
            self.ma = self.data.groupby('Datetime').min()['Price'] + self.data.groupby('Datetime').max()['Price']
    
    def get_values(self):
        return {"Moving Average": self.ma.values[0]}