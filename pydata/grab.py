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
class Grab(object):
    def __init__(self, args):
        ## flag of if need to reload all stock data
        self.reload_data = args.reload_data
        ## type of output file json/csv or both
        self.output_type = args.output_type
        ## charset of output file utf-8/gbk
        self.charset = args.charset
        ## portfolio testing date range(# of days)
        #self.test_date_range = args.test_date_range
        ## stock data loading start date(e.g. 2014-09-14)
        self.start_date = args.start_date
        ## stock data loading end date
        self.end_date = args.end_date
        ## portfolio generating target date
        self.target_date = args.target_date
        ## thread number
        self.thread = args.thread
        ## data file store path
        if(args.store_path == 'USER_HOME/tmp/pydata_export'):
            self.export_folder = os.path.expanduser('~') + '/tmp/pydata_export'
        else:
            self.export_folder = args.store_path
        ## for getting quote symbols
        self.all_quotes_url = 'http://money.finance.sina.com.cn/d/api/openapi_proxy.php'
        ## for loading quote data
        self.yql_url = 'http://query.yahooapis.com/v1/public/yql'
        ## export file name
        self.export_file_name = 'pydata_export'
        self.index_array = ['000001.SS', '399001.SZ', '000300.SS']
        self.sh000001 = {'Symbol': '000001.SS', 'Name': '上证指数'}
        self.sz399001 = {'Symbol': '399001.SZ', 'Name': '深证成指'}
        self.sh000300 = {'Symbol': '000300.SS', 'Name': '沪深300'}
        ## self.sz399005 = {'Symbol': '399005.SZ', 'Name': '中小板指'}
        ## self.sz399006 = {'Symbol': '399006.SZ', 'Name': '创业板指'}
        #self.collection_name = 'testing_method'
        self.all_quotes_info=[]
        self.all_quotes_data=[]
        
    def load_all_quote_symbol(self):
        print("load_all_quote_symbol start..." + "\n")
        start = timeit.default_timer()
        all_quotes = []
        all_quotes.append(self.sh000001)
        all_quotes.append(self.sz399001)
        all_quotes.append(self.sh000300)
        ## ggall_quotes.append(self.sz399005)
        ## all_quotes.append(self.sz399006)
        try:
            count = 1
            while (count < 100):
                para_val = '[["hq","hs_a","",0,' + str(count) + ',500]]'
                r_params = {'__s': para_val}
                r = requests.get(self.all_quotes_url, params=r_params)
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
        print("total " + str(len(all_quotes)) + " quotes are loaded..." + "\n")
        return all_quotes

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
                self.data_save_one(quote)
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

    def data_export(self, all_quotes, export_type_array, file_name):
        start = timeit.default_timer()
        directory = self.export_folder
        if(file_name is None):
            file_name = self.export_file_name
        if not os.path.exists(directory):
            os.makedirs(directory)
        if(all_quotes is None or len(all_quotes) == 0):
            print("no data to export...\n")
        if('json' in export_type_array):
            print("start export to JSON file...\n")
            f = io.open(directory + '/' + file_name + '.json', 'w', encoding=self.charset)
            json.dump(all_quotes, f, ensure_ascii=False)
        if('csv' in export_type_array):
            print("start export to CSV file...\n")
            columns = []
            if(all_quotes is not None and len(all_quotes) > 0):
                columns = self.get_columns(all_quotes[0])
            writer = csv.writer(open(directory + '/' + file_name + '.csv', 'w', encoding=self.charset))
            writer.writerow(columns)
            for quote in all_quotes:
                if('Data' in quote):
                    for quote_data in quote['Data']:
                        try:
                            line = []
                            for column in columns:
                                if(column.find('data.') > -1):
                                    if(column[5:] in quote_data):
                                        line.append(quote_data[column[5:]])
                                else:
                                    line.append(quote[column])
                            writer.writerow(line)
                        except Exception as e:
                            print(e)
                            print("write csv error: " + quote)
        print("export is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")

    def get_columns(self, quote):
        columns = []
        if(quote is not None):
            for key in quote.keys():
                if(key == 'Data'):
                    for data_key in quote['Data'][-1]:
                        columns.append("data." + data_key)
                else:
                    columns.append(key)
            columns.sort()
        return columns

    def read_csv_file(self, all_quotes,file_name=None):
        start = timeit.default_timer()
        directory = self.export_folder
        if(file_name is None):
            file_name = self.export_file_name
        st=pd.read_csv(directory + '/' + file_name + '.csv')
        return st

    def data_load(self, start_date, end_date, output_types):
        all_quotes = self.load_all_quote_symbol()
        #df=pd.read_csv('allsymbols.csv')
        #all_quotes=df.to_dict('records')
        #self.convert_allinone_dtyp()
        #writeSqlPD(self.allInOne,'MKTNewest')
        some_quotes = all_quotes
        self.load_all_quote_data(some_quotes, start_date, end_date)
        self.data_export(all_quotes, output_types, None)

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

    def data_save(self,all_quotes):
        #df=pd.DataFrame(all_quotes)
        #print("Stocks to be saved to DB: \n",df)
        start = timeit.default_timer()
        count=1
        for quote in all_quotes:
            if ('Data' in quote and 'Symbol' in quote):
                df=pd.DataFrame(quote['Data'])
                self.all_quotes_data.append(df)
                print("stock ",quote['Symbol']," save to db")
                self.convert_onestock_dtype(df)
                #writeSqlPD(df,quote['Symbol'])
                updateSqlPD(df,quote['Symbol'])
                count=count+1
            #if(count==5):
            #    break
        print("%d stocks have saved to DB\n"%count)
        print("save data to DB end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")

    def data_save_one(self,quote):
        if ('Data' in quote and 'Symbol' in quote):
            df=pd.DataFrame(quote['Data'])
            self.all_quotes_data.append(df)
            print("stock ",quote['Symbol']," save to db")
            self.convert_onestock_dtype(df)
            #writeSqlPD(df,quote['Symbol'])
            updateSqlPD(df,quote['Symbol'])


    def run(self):
        ## output types
        output_types = []
        if(self.output_type == "json"):
            output_types.append("json")
        elif(self.output_type == "csv"):
            output_types.append("csv")
        elif(self.output_type == "all"):
            output_types = ["json", "csv"]
        init() 
        ## loading stock data
        if(self.reload_data == 'Y'):
            self.data_load(self.start_date, self.end_date, output_types)
            
