import os
import re
from io import StringIO

from datetime import datetime
from datetime import timedelta
import scrapy
from quotespider import utils
from scrapy.selector import Selector
import pandas as pd
import numpy as np


def parse_date(date):
    if date:
        # date = datetime.strptime(date_str, '%Y%m%d')
        return date.strftime("%s")
    return None


def make_url(symbol, start_date=None, end_date=None):
    # url = ('http://ichart.finance.yahoo.com/table.csv?'
    #        's=%(symbol)s&d=%(end_month)s&e=%(end_day)s&f=%(end_year)s&g=d&'
    #        'a=%(start_month)s&b=%(start_day)s&c=%(start_year)s&ignore=.csv')
    url = ('https://finance.yahoo.com/quote/%(symbol)s/history?'
           'period1=%(start_date)s&period2=%(end_date)s&interval=1d&filter=history&frequency=1d')
    if not end_date:
        end_date = datetime.today().date()
    if not start_date:
        start_date = end_date - timedelta(days=365)

    startdate = start_date.strftime("%s")
    enddate = end_date.strftime("%s")

    # return url % {
    #     'symbol': symbol,
    #     'start_year': start_date[0],
    #     'start_month': start_date[1],
    #     'start_day': start_date[2],
    #     'end_year': end_date[0],
    #     'end_month': end_date[1],
    #     'end_day': end_date[2]
    # }

    print('=====start=======', startdate)
    print('=====end=======', enddate)
    start_url = url % {
        'symbol': symbol,
        'start_date': startdate,
        'end_date': enddate
    }
    print('=====start url=========', start_url)
    return start_url


def generate_urls(symbols, start_date=None, end_date=None):
    for symbol in symbols:
        yield make_url(symbol, start_date, end_date)


class YahooSpider(scrapy.Spider):
    name = 'yahoo'
    allowed_domains = ['finance.yahoo.com']

    def __init__(self, **kwargs):
        super(YahooSpider, self).__init__(**kwargs)

        symbols_arg = kwargs.get('symbols')
        start_date = kwargs.get('startdate', '')
        end_date = kwargs.get('enddate', '')

        utils.check_date_arg(start_date, 'startdate')
        utils.check_date_arg(end_date, 'enddate')

        if symbols_arg:
            if os.path.exists(symbols_arg):
                # get symbols from a text file
                symbols = utils.load_symbols(symbols_arg)
            else:
                # inline symbols in command
                symbols = symbols_arg.split(',')
            self.start_urls = generate_urls(symbols, start_date, end_date)
        else:
            self.start_urls = []


    def parse(self, response):
        # head = Selector(response=response).xpath('//thead/tr/th/span/text()').extract()
        head = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']
        th = pd.Series(head).str.lower()
        items = Selector(response=response).xpath('//tr/td/span/text() | //tr/td[not(span)]').extract()[:-1]
        data = pd.DataFrame(np.reshape(np.array(items), (100, 7)), columns=th)
        symbol = self._get_symbol_from_url(response.url)
        data['code'] = symbol
        data.date = pd.to_datetime(data.date)
        data.date = data.date.dt.strftime('%Y-%m-%d')
        yield data.to_dict('list')

    def _get_symbol_from_url(self, url):
        match = re.search(r'[0-9]{6}\.[SZ]{2}', url)
        if match:
            return match.group(0)
        return ''


if '__name__' == '__main__':
    spider = YahooSpider(symbols=['603999.SS'])
