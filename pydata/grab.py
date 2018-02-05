#!/usr/bin/env python
# coding=utf-8
import requests
import json
import option
from datetime import datetime
from datetime import timedelta
import timeit
import time
import io
import os
import csv
import re
# from pymongo import MongoClient
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from liteDB import *
import pandas as pd
from const import *
import tushare as ts
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
        # self.test_date_range = args.test_date_range
        ## stock data loading start date(e.g. 2014-09-14)
        self.start_date = args.start_date
        ## stock data loading end date
        self.end_date = args.end_date
        ## portfolio generating target date
        self.target_date = args.target_date
        self.CRAWL_FILE_NAME = 'crawl' + self.start_date.replace('-', '') + '_' + self.end_date.replace('-', '') + '.csv'
        ## thread number
        self.thread = args.thread
        ## data file store path
        if (args.store_path == 'USER_HOME/tmp/pydata_export'):
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
        # self.collection_name = 'testing_method'
        self.all_quotes_fail = [['Symbol', 'Name']]
        self.all_quotes_data = []
        self.update = args.update
        main(args)
        self.updateone = args.updateone
        self.symbol = args.symbol

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
                col = r.json()[0]['fields']
                itm = r.json()[0]['items']
                if (count == 1):
                    self.allInOne = pd.DataFrame(itm, columns=col)
                else:
                    self.allInOne = self.allInOne.append(pd.DataFrame(itm, columns=col))
                if (len(r.json()[0]['items']) == 0):
                    break
                for item in r.json()[0]['items']:
                    quote = {}
                    code = item[0]
                    name = item[2]
                    ## convert quote code
                    if (code.find('sh') > -1):
                        code = code[2:] + '.SS'
                    elif (code.find('sz') > -1):
                        code = code[2:] + '.SZ'
                    ## convert quote code end
                    quote['Symbol'] = code
                    quote['Name'] = name
                    print("load " + code + " " + name)
                    all_quotes.append(quote)
                count += 1
        except Exception as e:
            print("Error: Failed to load all stock symbol..." + "\n")
            print(e)
        print("load_all_quote_symbol end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
        print("total " + str(len(all_quotes)) + " quotes are loaded..." + "\n")
        return all_quotes

    def load_quote_info(self, quote, is_retry):
        # print("load_quote_info start..." + "\n")
        start = timeit.default_timer()
        if (quote is not None and quote['Symbol'] is not None):
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
            except Exception as e:
                print("Error: Failed to load stock info... " + quote['Symbol'] + "/" + quote['Name'] + "\n")
                print(e + "\n")
                if (not is_retry):
                    time.sleep(1)
                    self.load_quote_info(quote, True)  ## retry once for network issue
        # print(quote)
        # print("load_quote_info end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
        return quote

    def load_all_quote_info(self, all_quotes):
        print("load_all_quote_info start...")

        start = timeit.default_timer()
        for idx, quote in enumerate(all_quotes):
            print("#" + str(idx + 1))
            self.load_quote_info(quote, False)

        print("load_all_quote_info end... time cost: " + str(round(timeit.default_timer() - start)) + "s")
        return all_quotes

    def get_whole_quote_hist(self, symbol):
        '''
        symbol: string, 600000.SS
        '''
        now = datetime.now()
        data = []
        while True:
            start_datetime = now
            start_date = start_datetime.strftime(DATEFORMAT)
            end_date = (start_datetime - timedelta(365)).strftime(DATEFORMAT)
            yquery = 'select * from yahoo.finance.historicaldata where symbol = "' + symbol.upper() + '" and startDate = "' + end_date + '" and endDate = "' + start_date + '"'
            r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
            quote_data = []
            try:
                r = requests.get(self.yql_url, params=r_params)
                rjson = r.json()
                quote_data = rjson['query']['results']['quote']
                quote_data.reverse()
                print("load symbol " + symbol + ' at year ' + start_date)
            except:
                print("Error: Failed to load stock data... " + symbol + " " + start_date + "\n")
                break
            data = data + quote_data
            now = now - timedelta(365)
        return self.convert2DataFrame(data)

    def get_quote_hist(self, symbol):
        '''
        symbol: string, 600000.SS
        '''
        now = datetime.strptime(self.end_date, DATEFORMAT)
        data = []
        pivot = datetime.strptime(self.start_date, DATEFORMAT)
        stop = False
        while True:
            end_datetime = now
            start_datetime = end_datetime - timedelta(365)
            start_date = start_datetime.strftime(DATEFORMAT)
            end_date = end_datetime.strftime(DATEFORMAT)
            if start_datetime <= pivot:
                start_date = self.start_date
                stop = True
            # quote_data=self.get_oneyear_quote(symbol,start_date,end_date)
            yquery = 'select * from yahoo.finance.historicaldata where symbol = "' + symbol.upper() + '" and startDate = "' + start_date + '" and endDate = "' + end_date + '"'
            # r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
            r_params = {'q': yquery, 'format': 'json', 'env': 'store://datatables.org/alltableswithkeys'}
            quote_data = []
            try:
                r = requests.get(self.yql_url, params=r_params)
                rjson = r.json()
                quote_data = rjson['query']['results']['quote']
                quote_data.reverse()
                print("load symbol " + symbol + ' at year ' + start_date)
            except:
                print("Error: Failed to load stock data... " + symbol + " " + start_date + "\n")
                break
            data = data + quote_data
            if stop == True:
                break
            now = now - timedelta(365)
        return self.convert2DataFrame(data)

    def convert2DataFrame(self, data):
        '''
        data: [dict1,dict2,...] dict-like list
        return DataFrame
        '''
        if len(data) == 0:
            return None
        df = pd.DataFrame.from_dict(data)
        # df=df.drop_duplicates()
        df = df[OHLC_HEAD]
        # df=df.sort(columns='Date',ascending=False)
        return df

    def get_oneyear_quote(self, symbol, start_date, end_date):
        '''
        symbol: string, 600000.SS
        start_date: datetime
        '''
        # start=datetime.strptime(start_date,DATEFORMAT)
        # end=datetime.strptime(end_date,DATEFORMAT)
        # end=(start-timedelta(365)).strftime(DATEFORMAT)
        quote = {}
        quote['Symbol'] = symbol
        quote['Name'] = ''
        self.load_quote_data(quote, start_date, end_date, True, [])
        print('load ' + symbol + '    ' + start_date + '-----' + end_date)
        return quote['Data']

    def load_quote_data(self, quote, start_date, end_date, is_retry, counter, source):
        start = timeit.default_timer()
        if (quote is not None and 'Symbol' in quote):
            quote = self.load_onequote_yahoo(quote, start_date, end_date)
            yquery = 'select * from yahoo.finance.historicaldata where symbol = "' + quote[
                'Symbol'].upper() + '" and startDate = "' + start_date + '" and endDate = "' + end_date + '"'
            # r_params = {'q': yquery, 'format': 'json', 'env': 'http://datatables.org/alltables.env'}
            r_params = {'q': yquery, 'format': 'json', 'env': 'store://datatables.org/alltableswithkeys'}
            try:
                r = requests.get(self.yql_url, params=r_params)
                # print(r.url)
                rjson = r.json()
                quote_data = rjson['query']['results']['quote']
                quote_data.reverse()
                quote['Data'] = quote_data
                self.data_save_one(quote)
                if (not is_retry):
                    counter.append(1)
            except:
                print("Error: Failed to load stock data... " + quote['Symbol'] + "/" + quote['Name'] + "\n")
                self.all_quotes_fail.append([quote['Symbol'], quote['Name']])
                if (not is_retry):
                    time.sleep(2)
                    self.load_quote_data(quote, start_date, end_date, True, counter)  ## retry once for network issue
        return quote

    def load_all_quote_data_tushare(self, all_quotes, start_date, end_date):
        print("quotes: ", all_quotes)
        data = ts.get_hists(all_quotes, start_date, end_date, retry_count=2)
        return data

    def load_all_quote_data_yahoo(self, all_quotes, start_date, end_date):
        print("load_all_quote_data start...start:%s , end:%s,\n" % (start_date, end_date))
        start = timeit.default_timer()
        counter = []
        mapfunc = partial(self.load_quote_data, start_date=start_date, end_date=end_date, is_retry=True,
                          counter=counter)
        pool = ThreadPool(self.thread)
        pool.map(mapfunc, all_quotes)  ## multi-threads executing
        pool.close()
        pool.join()
        print("load_all_quote_data end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
        l = len(all_quotes)
        print("loaded %d stocks data: \n" % l)
        return all_quotes

    def data_export(self, all_quotes, export_type_array, file_name):
        start = timeit.default_timer()
        directory = self.export_folder
        if (file_name is None):
            file_name = self.export_file_name
        if not os.path.exists(directory):
            os.makedirs(directory)
        if (all_quotes is None or len(all_quotes) == 0):
            print("no data to export...\n")
        if ('json' in export_type_array):
            print("start export to JSON file...\n")
            f = io.open(directory + '/' + file_name + '.json', 'w', encoding=self.charset)
            json.dump(all_quotes, f, ensure_ascii=False)
        if ('csv' in export_type_array):
            print("start export to CSV file...\n")
            columns = []
            self.all_quotes_data = []
            for quote in all_quotes:
                if ('Data' in quote):
                    self.all_quotes_data = self.all_quotes_data + quote['Data']
        print("export is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")

    def get_columns(self, quote):
        columns = []
        if (quote is not None):
            for key in quote.keys():
                if (key == 'Data'):
                    for data_key in quote['Data'][-1]:
                        columns.append("data." + data_key)
                else:
                    columns.append(key)
            columns.sort()
        return columns

    def read_csv_file(self, all_quotes, file_name=None):
        start = timeit.default_timer()
        directory = self.export_folder
        if (file_name is None):
            file_name = self.export_file_name
        st = pd.read_csv(directory + '/' + file_name + '.csv')
        return st

    def data_load(self, start_date, end_date, output_types):
        # all_quotes = self.load_all_quote_symbol()
        df = pd.read_csv(SYMBOL_FILE)
        all_quotes = df.to_dict('records')
        some_quotes = all_quotes[:]
        # self.load_all_quote_info(some_quotes)
        self.load_all_quote_data(df.Symbol, start_date, end_date)
        self.data_export(some_quotes, output_types, None)
        df = self.convert2DataFrame(self.all_quotes_data)
        fails = pd.DataFrame(self.all_quotes_fail, columns=['symbol', 'name'])
        fails.to_csv(FAIL_RECORDS_FILE)
        return df

    def data_load_tushare(self, start_date, end_date):
        df = pd.read_csv(SYMBOL_FILE)
        df = init_data_set(SYMBOL_FILE)
        symbls = df.code.drop_duplicates()
        data = pd.DataFrame()
        retry = pd.DataFrame()
        fail_symbls = []
        data, fail_symbls = self.load_all_quote_data_tushare(symbls, start_date, end_date)
        while len(fail_symbls) > 50:
            retry, fail_symbls = self.load_all_quote_data_tushare(fail_symbls, start_date, end_date)
            data.append(retry)
        print(" failed to download " + str(len(fail_symbls)) + " symbols")
        fail_symbls.to_csv(FAIL_RECORDS_FILE)
        return data

    def try_elim_fail(self, start_date, end_date, output_types):
        df = pd.read_csv(FAIL_RECORDS_FILE)
        df = init_data_set(FAIL_RECORDS_FILE)
        symbls = df[BASCIC_KEY]
        data, fail_symbls = self.load_all_quote_data_tushare(symbls, start_date, end_date)
        print(" failed to download " + str(len(fail_symbls)) + " symbols")
        data.to_csv('reload.csv')
        return data

    def convert_allinone_dtyp(self):
        self.allinone['code'] = self.allinone['code'].astype('str')
        self.allinone['name'] = self.allinone['name'].astype('str')
        self.allinone['ticktime'] = pd.to_datetime(self.allinone['ticktime'])
        self.allinone[
            ['trade', 'pricechange', 'changepercent', 'buy', 'sell', 'settlement', 'open', 'high', 'low', 'volume',
             'amount', 'nta']] = self.allinone[
            ['trade', 'pricechange', 'changepercent', 'buy', 'sell', 'settlement', 'open', 'high', 'low', 'volume',
             'amount', 'nta']].astype('float')

    def convert_onestock_dtype(self, quote):
        # print("single stock dtype before adjusting:\n",quote.dtypes)
        quote[['Adj_Close', 'Close', 'High', 'Low', 'Open']] = quote[
            ['Adj_Close', 'Close', 'High', 'Low', 'Open']].astype('float')
        quote['Volume'] = quote['Volume'].astype('int')
        quote['Date'] = pd.to_datetime(quote['Date'])

    # print("single stock dtype after adjusting:\n",quote.dtypes)

    def data_save(self, all_quotes):
        # df=pd.DataFrame(all_quotes)
        # print("Stocks to be saved to DB: \n",df)
        start = timeit.default_timer()
        count = 1
        for quote in all_quotes:
            if ('Data' in quote and 'Symbol' in quote):
                df = pd.DataFrame(quote['Data'])
                self.all_quotes_data.append(df)
                print("stock ", quote['Symbol'], " save to db")
                self.convert_onestock_dtype(df)
                # writeSqlPD(df,quote['Symbol'])
                updateSqlPD(df, quote['Symbol'])
                count = count + 1
            # if(count==5):
            #    break
        print("%d stocks have saved to DB\n" % count)
        print("save data to DB end... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")

    def data_save_one(self, quote):
        if 'Data' in quote and 'Symbol' in quote:
            df = pd.DataFrame(quote['Data'])
            self.all_quotes_data.append(df)
            print("stock ", quote['Symbol'], " save to db")
            self.convert_onestock_dtype(df)
            # writeSqlPD(df,quote['Symbol'])
            updateSqlPD(df, quote['Symbol'])

    def run(self):
        ## output types
        start = timeit.default_timer()
        print("==============startdate============= ", self.start_date)
        print("==============enddate=============== ", self.end_date)
        output_types = []
        self.reload_data = 'Y'
        self.start_date = '2017-01-11'
        if self.output_type == "json":
            output_types.append("json")
        elif self.output_type == "csv":
            output_types.append("csv")
        elif self.output_type == "all":
            output_types = ["json", "csv"]
        # res=pd.DataFrame([],columns=DATA_HEAD)
        res = pd.DataFrame()
        if self.update == 'Y':
            res = self.data_load_tushare(self.start_date, self.end_date)
        elif self.updateone == 'all':
            res = self.get_whole_quote_hist(self.symbol)
            res.to_csv(symbl2num(self.symbol) + '.csv')
            return
        elif self.updateone == 'range':
            res = self.get_quote_hist(self.symbol)
            print(res)
            res.to_csv(symbl2num(self.symbol) + '.csv')
            return
        if self.reload_data == 'Y':
            f = self.try_elim_fail(self.start_date, self.end_date, output_types)
            q = pd.DataFrame.from_csv(self.CRAWL_FILE_NAME)
            res = pd.concat((q, f), ignore_index=True)
            res.drop_duplicates(inplace=True)
        if res is not None:
            res.to_csv(self.CRAWL_FILE_NAME)
        else:
            print("!!!!!!!!!!!!!!!!!!!!!result is nulll!!!!!!!!!!!!!!!!")
        print("Grab finish in:  " + str(round(timeit.default_timer() - start)) + "s" + "\n")
