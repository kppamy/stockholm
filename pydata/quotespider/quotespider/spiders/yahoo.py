import os
import re
from datetime import datetime
from datetime import timedelta
import scrapy
from quotespider import sutils
from scrapy.selector import Selector
from scrapy.log import logging
import pandas as pd
import numpy as np
from scrapy.http import Request

# Example data format:
# Date	Open	High	Low	Close*	Adj Close**	Volume
# Sep 14, 2018	3.05	3.06	3.03	3.04	3.04	1,755,000

class YahooSpider(scrapy.Spider):
    name = 'yahoo'
    allowed_domains = ['finance.yahoo.com']
    # handle_httpstatus_list = [307, 302]


    def __init__(self, **kwargs):
        super(YahooSpider, self).__init__(**kwargs)

        logging.getLogger('scrapy').setLevel(logging.WARNING)

        symbols_arg = kwargs.get('symbols')
        start_date = kwargs.get('startdate', '')
        end_date = kwargs.get('enddate', '')
        self.count = 0
        self.fail = 0
        self.fail_symbols = []
        self.data = pd.DataFrame()
        self.start_date = start_date
        self.end_date = end_date

        sutils.check_date_arg(start_date, 'startdate')
        sutils.check_date_arg(end_date, 'enddate')

        # symbols_arg = '/Users/chenay/pyt/pydata/pydata/allsymbols.csv'

        if symbols_arg:
            symbols_path = os.path.abspath(symbols_arg)
            if os.path.exists(symbols_path):
                # get symbols from a text file
                symbols = sutils.load_symbols(symbols_path)
            else:
                # inline symbols in command
                symbols = symbols_arg.split(',')
            self.start_urls = self.generate_urls(symbols, start_date, end_date)
        else:
            self.start_urls = []

    # def make_requests_from_url(self, url):
    #     return Request(url, dont_filter=True, meta = {
    #               'dont_redirect': True,
    #               'handle_httpstatus_list': [302, 307]
    #         })

    def parse(self, response):
        # head = Selector(response=response).xpath('//thead/tr/th/span/text()').extract()
        head = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']
        th = pd.Series(head).str.lower()
        items = Selector(response=response).xpath('//section//tr/td/span/text() | '
                                                  '//tr/td[not(span)]/text()').extract()[:-1]
        items = pd.Series(items)
        items = sutils.find_first_ohlc(items)
        symbol = self._get_symbol_from_url(response.url)
        if items is None or len(items) == 0:
            self.fail = self.fail + 1
            logging.info('failed to download ' + symbol + ' No.' + str(self.fail))
            self.fail_symbols.append(symbol)
            return None
        sutils.drop_trash(items)
        items = items.tolist()
        rows = len(items)//7
        data = pd.DataFrame(np.reshape(items[:(rows*7)], (rows, 7)), columns=th)
        data['code'] = symbol
        try:
            data.date = pd.to_datetime(data.date)
        except ValueError:
            # data.to_csv(symbol+'.csv')
            self.fail = self.fail + 1
            logging.info('failed to download ' + symbol + ' No.' + str(self.fail))
            self.fail_symbols.append(symbol)
            raise ValueError("symbol '%s'" % symbol)
        data.date = data.date.dt.strftime('%Y-%m-%d')
        if len(data) > 0:
            data = sutils.to_tushare(data)
            # yield data.to_dict('list')
            self.data = self.data.append(data)
            self.count = self.count + 1
            logging.info(symbol + '  success No.' + str(self.count))
        else :
            logging.info(symbol + '  data is empty')

    def _get_symbol_from_url(self, url):
        match = re.search(r'[0-9]{6}\.[SZ]{2}', url)
        if match:
            return match.group(0)
        return ''

    def closed(self, reason):
        self.data.to_csv('yahoo'+self.start_date+'_'+self.end_date+'.csv')
        pd.Series(self.fail_symbols).to_csv('yahoo_fail'+self.end_date+'.csv')

    def make_url(self, symbol, start_date=None, end_date=None):
        url = ('https://finance.yahoo.com/quote/%(symbol)s/history?'
               'period1=%(start_date)s&period2=%(end_date)s&interval=1d&filter=history&frequency=1d')
        start_date = sutils.parse_date_fromstr(start_date)
        end_date = sutils.parse_date_fromstr(end_date)
        if not end_date:
            end_date = datetime.today().date()
        if not start_date:
            start_date = end_date - timedelta(days=365)
        startdate = start_date.strftime("%s")
        enddate = end_date.strftime("%s")
        self.end_date = end_date.strftime("%Y%m%d")
        start_url = url % {
            'symbol': symbol,
            'start_date': startdate,
            'end_date': enddate
        }
        return start_url

    def generate_urls(self, symbols, start_date=None, end_date=None):
        for symbol in symbols:
            yield self.make_url(symbol, start_date, end_date)


# from scrapy.cmdline import execute
# execute("scrapy crawl yahoo -a symbols=../../../allsymbols.csv -a startdate=20180622 ".split())
# execute("scrapy crawl yahoo -a symbols=600432.SS,600806.SS,000511.S -a startdate=20190419".split())
