#!/usr/bin/env pythonpandas_datareader
# coding=utf-8

from IPython.core.display import *

import datetime
import pandas as pd
from pandas_datareader import data, wb
from pandas import Series, DataFrame
import matplotlib.pyplot as plt
import matplotlib as mpl
#mpl.use('Agg')

display(HTML("<iframe src=http://pandas.pydata.org width=800 height=350></iframe>"))
print("pandas version: \n",pd.__version__)

mpl.rc('figure', figsize=(8, 7))
#print("matplotlib versiopn:p \n",mpl.__version__)

labels = ['a', 'b', 'c', 'd', 'e']
s = Series([1, 2, 3, 4, 5], index=labels)
mapping=s.to_dict()
#print("from Series to dict: \n",mapping)
s2=Series(mapping)
#print("from dict to Series \n",s2)

#aapl = data.get_data_yahoo('AAPL', 
#                                 start=datetime.datetime(2015, 1, 16), 
#                                 end=datetime.datetime.today())
#print("get data from yahoo finance\n",aapl.head())

#aapl.to_csv('aapl_ohlc.csv')
df = pd.read_csv('aapl_ohlc.csv', index_col='Date', parse_dates=True)
print("to csv and recover the data: \n",df.head())

ts = df['Close'][-10:]
print("close price in last 10 days: \n",ts)
close_px = df['Adj Close']
mavg = pd.rolling_mean(close_px, 40)
#print("rolling mean of 40 days: \n ",mavg)

#rets = close_px / close_px.shift(1) - 1
#print("close prince change in percent: \n",rets)
print("close prince change in percent by pct_change(): \n",close_px.pct_change().head())

close_px.plot(label='AAPL')
mavg.plot(label='mavg')
plt.legend()
plt.show()
#plt.savefig('MyFig.jpg')  
