#!/usr/bin/env python
# coding=utf-8
import requests
import json
import datetime
import timeit
import time
import io
import os
import csv
import re
#from pymongo import MongoClient
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from liteDB import *
import pandas as pd
import numpy as np
class Grab(Object):
    def load_all_quote_symbol(self):
        print("load_all_quote_symbol start..." + "\n")

        start = timeit.default_timer()

        all_quotes = []
        #all_stocks=[]

        all_quotes.append(self.sh000001)
        all_quotes.append(self.sz399001)
        all_quotes.append(self.sh000300)
        ## all_quotes.append(self.sz399005)
        ## all_quotes.append(self.sz399006)

        try:
            count = 1
            while (count < 100):
                para_val = '[["hq","hs_a","",0,' + str(count) + ',500]]'
                r_params = {'__s': para_val}
                r = requests.get(self.all_quotes_url, params=r_params)
                #print("all Symbol jason:\n",r.json()[0]['fields'])
                col=r.json()[0]['fields']
                itm=r.json()[0]['items']
                if(count == 1):
                    self.allInOne=pd.DataFrame(itm,columns=col)
                else:
                    self.allInOne=self.allInOne.append(pd.DataFrame(itm,columns=col))
                    if(len(r.json()[0]['items']) == 0):
                        break
                for item in r.json()[0]['items']:
                    quote = {}
                    code = item[0]
                    name = item[2]
                    ## convert quote code
                    if(code.find('sh') > -1):
                        code = code[2:] + '.SS'
                    elif(code.find('sz') > -1):
                        code = code[2:] + '.SZ'
                        ## convert quote code end
                        quote['Symbol'] = code
                        quote['Name'] = name
                        print("load "+code+" "+name)
                        all_quotes.append(quote)
                        count += 1
        except Exception as e:
            print("Error: Failed to load all stock symbol..." + "\n")
            print(e)
            print("load_all_quote_symbol end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
            return all_quotes

    def load_sectors_info(self, quote, is_retry):
        print("load_sectors_info start..." + "\n")

        start = timeit.default_timer()

        if(quote is not None):
            yquery = 'select * from yahoo.finance.industry where id="881150"'
            r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
            r = requests.get(self.yql_url, params=r_params)
            rjson = r.json()
            print("all sectors json: \n",rjson)
            try:
                quote_info = rjson['query']['results']['quote']
                print("all sectors info: \n",quote_info)
                #quote['LastTradeDate'] = quote_info['LastTradeDate']
                #quote['LastTradePrice'] = quote_info['LastTradePriceOnly']
                #quote['PreviousClose'] = quote_info['PreviousClose']
                #quote['Open'] = quote_info['Open']
                #quote['DaysLow'] = quote_info['DaysLow']
                #quote['DaysHigh'] = quote_info['DaysHigh']
                #quote['Change'] = quote_info['Change']
                #quote['ChangeinPercent'] = quote_info['ChangeinPercent']
                #quote['Volume'] = quote_info['Volume']
                #quote['MarketCap'] = quote_info['MarketCapitalization']
                #quote['StockExchange'] = quote_info['StockExchange']
                #quote['BookValue'] = quote_info['BookValue']
                #quote['YearHigh'] = quote_info['YearHigh']
                #quote['YearLow'] = quote_info['YearLow']
                #self.all_quotes_info.append(quote)
            except Exception as e:
                print("Error: Failed to load stock info... "  + "\n")
                #print(e + "\n")
                # if(not is_retry):
                #     time.sleep(1)
                #     load_quote_info(quote, True) ## retry once for network issue

        #print(quote)
        print("load_quote_info end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
        return quote
    def load_quote_info(self, quote, is_retry):
        #print("load_quote_info start..." + "\n")

        start = timeit.default_timer()

        if(quote is not None and quote['Symbol'] is not None):
            yquery = 'select * from yahoo.finance.quotes where symbol = "' + quote['Symbol'].lower() + '"'
            r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
            r = requests.get(self.yql_url, params=r_params)
            ## print(r.url)
            ## print(r.text)
            rjson = r.json()
            try:
                quote_info = rjson['query']['results']['quote']
                quote['LastTradeDate'] = quote_info['LastTradeDate']
                quote['LastTradePrice'] = quote_info['LastTradePriceOnly']
                quote['PreviousClose'] = quote_info['PreviousClose']
                quote['Open'] = quote_info['Open']
                quote['DaysLow'] = quote_info['DaysLow']
                quote['DaysHigh'] = quote_info['DaysHigh']
                quote['Change'] = quote_info['Change']
                quote['ChangeinPercent'] = quote_info['ChangeinPercent']
                quote['Volume'] = quote_info['Volume']
                quote['MarketCap'] = quote_info['MarketCapitalization']
                quote['StockExchange'] = quote_info['StockExchange']
                quote['BookValue'] = quote_info['BookValue']
                quote['YearHigh'] = quote_info['YearHigh']
                quote['YearLow'] = quote_info['YearLow']
                self.all_quotes_info.append(quote)
            except Exception as e:
                print("Error: Failed to load stock info... " + quote['Symbol'] + "/" + quote['Name'] + "\n")
                print(e + "\n")
                if(not is_retry):
                    time.sleep(1)
                    load_quote_info(quote, True) ## retry once for network issue

        #print(quote)
        #print("load_quote_info end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
        return quote

    def load_all_quote_info(self, all_quotes):
        print("load_all_quote_info start...")

        start = timeit.default_timer()
        for idx, quote in enumerate(all_quotes):
            print("#" + str(idx + 1),)
            self.load_quote_info(quote,False)

        print("load_all_quote_info end... time cost: " + str(round(timeit.default_timer() - start)) + "s")
        return all_quotes

    def load_all_between(self, quote, start_date, end_date, is_retry, counter):
        print("load_all_between start..." + "\n")

        start = timeit.default_timer()

        if(quote is not None ):        
            yquery = 'select * from yahoo.finance.historicaldata where startDate = "' + start_date + '" and endDate = "' + end_date + '"'
            r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
            try:
                r = requests.get(self.yql_url, params=r_params)
                rjson = r.json()
                print("quote data rjson\n",rjson)
                quote_data = rjson['query']['results']['quote']
                quote_data.reverse()
                quote['Data'] = quote_data
                print("load all data bettween : \n",quote_data)
                #self.data_save_one(quote)
                if(not is_retry):
                    counter.append(1)          
            except:
                print("Error: Failed to load stock data...  \n")
                if(not is_retry):
                    time.sleep(2)
                    self.load_all_between(quote, start_date, end_date, True, counter) ## retry once for network issue

        print("load_all_quote_data end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
        return quote


    def load_quote_data(self, quote, start_date, end_date, is_retry, counter):
        ## print("load_quote_data start..." + "\n")

        start = timeit.default_timer()

        if(quote is not None and quote['Symbol'] is not None):        
            yquery = 'select * from yahoo.finance.historicaldata where symbol = "' + quote['Symbol'].upper() + '" and startDate = "' + start_date + '" and endDate = "' + end_date + '"'
            r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
            try:
                r = requests.get(self.yql_url, params=r_params)
                ## print(r.url)
                ## print(r.text)
                rjson = r.json()
                #print("quote data rjson\n",rjson)
                quote_data = rjson['query']['results']['quote']
                quote_data.reverse()
                quote['Data'] = quote_data
                #print("one quote data from yahoo:\n",quote_data)
                #self.data_save_one(quote)
                if(not is_retry):
                    counter.append(1)          
            except:
                print("Error: Failed to load stock data... " + quote['Symbol'] + "/" + quote['Name'] + "\n")
                if(not is_retry):
                    time.sleep(2)
                    self.load_quote_data(quote, start_date, end_date, True, counter) ## retry once for network issue

        return quote

    def load_all_quote_data(self, all_quotes, start_date, end_date):
        print("load_all_quote_data start...start:%s , end:%s,\n"%(start_date,end_date))

        start = timeit.default_timer()

        counter = []
        mapfunc = partial(self.load_quote_data, start_date=start_date, end_date=end_date, is_retry=True, counter=counter)
        pool = ThreadPool(self.thread)
        pool.map(mapfunc, all_quotes) ## multi-threads executing
        pool.close() 
        pool.join()

        print("load_all_quote_data end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
        l=len(all_quotes)
        print("loaded %d stocks data: \n"%l)
        return all_quotes

    def data_load(self, start_date, end_date, output_types):
        all_quotes = self.load_all_quote_symbol()
        #print("total " + str(len(all_quotes)) + " quotes are loaded..." + "\n")
        #sectors={} 
        #self.load_sectors_info(sectors,False)
        #counter = []
        #self.load_all_between(all_quotes[0:10],start_date,end_date,False,counter)
        #self.convert_allinone_dtyp()
        #writeSqlPD(self.allInOne,'MKTNewest')
        #print("all quotes symbol: \n",all_quotes[0:10])
        #some_quotes = all_quotes
        some_quotes = all_quotes[0:10]
        #self.load_all_quote_info(some_quotes)
        #qi=pd.DataFrame(self.all_quotes_info)
        #print("all quotes info len : \n",qi.dropna(axis=1,how='all'))
        #print("all quotes info len : \n",qi)
        self.load_all_quote_data(some_quotes, start_date, end_date)
        #self.data_save(some_quotes)
        #qd=pd.DataFrame(self.all_quotes_data)
        # self.all_quotes_data.to_csv("alldata.csv")
        #self.all_quotes_data=pd.read_csv('alldata.csv',index_col='Date',parse_dates=True)
        #print("all quotes data: \n",self.all_quotes_data.head())
        ##self.data_process(all_quotes)

        #self.data_export(sectors, output_types, None)
        #self.data_export(some_quotes, output_types, None)
        st=self.read_csv_file(None,None,'industry')
        print("read concepts data:\n",st)
        #strategy=Strategy()
        #strategy.mark_all_down(st)

    def convert_allinone_dtyp(self):
        #print("all in one: dtypes before**********************************\n",self.allInOne.dtypes)
        self.allInOne['code']=self.allInOne['code'].astype('int')
        self.allInOne['name']=self.allInOne['name'].astype('str')
        self.allInOne['ticktime']=pd.to_datetime(self.allInOne['ticktime'])
        self.allInOne[['trade','pricechange','changepercent','buy','sell','settlement','open','high','low','volume','amount','nta']]=self.allInOne[['trade','pricechange','changepercent','buy','sell','settlement','open','high','low','volume','amount','nta']].astype('float')
        #print("all in one after ajust the dtypes:\n",self.allInOne.dtypes)

    def convert_onestock_dtype(self,quote):
       #print("single stock dtype before adjusting:\n",quote.dtypes)
        quote[['Adj_Close','Close','High','Low','Open']]= quote[['Adj_Close','Close','High','Low','Open']].astype('float')
        quote['Volume']=quote['Volume'].astype('int')
        quote['Date']=pd.to_datetime(quote['Date'])
       #print("single stock dtype after adjusting:\n",quote.dtypes)

