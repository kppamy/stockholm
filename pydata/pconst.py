#!/usr/bin/env python
# coding=utf-8
import timeit
import pandas as pd
from pandas.tseries.offsets import *
from datetime import datetime

DATEFORMAT = '%Y-%m-%d'
OHLC_HEAD = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']
BASIC_HEAD = ['date', 'open', 'high', 'low', 'close', 'volume', 'code']
# date head used by tushare : get_hists, get_hist_data, basic daily ticket date
TOHLC_HEAD = ['close', 'code', 'date', 'high', 'low', 'ma10', 'ma20', 'ma5', 'open', 'p_change',
                      'price_change', 'turnover', 'v_ma10', 'v_ma20', 'v_ma5', 'volume']
SYMINFO_HEAD = ['industry_code', 'industry', 'code', 'name', 'area', 'concept_code', 'concept_name']
FINANCE_HEAD = ['code', 'name', 'industry', 'area', 'pe', 'outstanding', 'totals', 'totalassets', 'liquidassets',
                'fixedassets', 'reserved', 'reservedpershare', 'esp', 'bvps', 'pb', 'timetomarket', 'undp', 'perundp',
                'rev', 'profit', 'gpr', 'npr', 'holders']
MIN_HEAD = ['code', 'name']
CODE_DTYPE = {'code': 'str'}
BASCIC_KEY = 'code'
SOURCE_DIR = '/Users/chenay/pyt/pydata/pydata/'
BASIC_DATA_FILE = SOURCE_DIR + 'basic.csv'
FOUNDAMENTAL_FILE = 'fundamental.csv'
OUTPUT_DATA_FILE = 'data.csv'
SYMBOL_FILE = 'allsymbols.csv'
FAIL_RECORDS_FILE = 'fail.csv'
FINANCE_REPORTS_FILE = 'reports.csv'
ANNUAL_FINANCE_REPORTS_FILE = 'annual_reports'