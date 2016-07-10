#!/usr/bin/env python
# coding=utf-8
from pandas_datareader import data as wb
from pandas import DataFrame
import timeit
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial

def load_all_quote_data(all_quotes, start_date, end_date):
        print("load_all_quote_data start...start:%s , end:%s,\n"%(start_date,end_date))
        
        start = timeit.default_timer()

        counter = []
        mapfunc = partial(load_quote_data, start_date=start_date, end_date=end_date, is_retry=False, counter=counter)
        pool = ThreadPool(10)
        pool.map(mapfunc, all_quotes) ## multi-threads executing
        pool.close() 
        pool.join()

def load_quote_data(quote, start_date, end_date, is_retry, counter):
    data_feed[quote] = wb.get_data_yahoo(quote, '01/01/2015', '07/08/2016')

data_feed={}
symbols=['AAPL']
#symbols=['AAPL','FB', 'GOOG', 'SPLK', 'YELP', 'GG','BP','SCPJ','JNJ', 'OMG']
start=timeit.default_timer()

load_all_quote_data(symbols,'01/01/2015', '07/08/2016')
#for ticker in symbols:
#    data_feed[ticker] = wb.get_data_yahoo(ticker, '01/01/2015', '07/08/2016')


print("load all info end... time cost: " + str(round(timeit.default_timer() - start)) + "s")


price = DataFrame({tic: data['Adj Close'] for tic, data in data_feed.iteritems()})
volume = DataFrame({tic: data['Volume'] for tic, data in data_feed.iteritems()})
returns = price.pct_change()

import matplotlib.pyplot as plt
turns.sum().plot(kind='bar',title="% return For Year")
plt.show()

