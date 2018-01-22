#!/usr/bin/env python
# coding=utf-8
import timeit
import pandas as pd

DATEFORMAT = '%Y-%m-%d'
OHLC_HEAD = ['date', 'open', 'high', 'low', 'close', 'volume', 'adj_close', 'symbol']
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
BASIC_DATA_FILE = 'basic.csv'
FINANCE_FILE = 'finance.csv'
OUTPUT_DATA_FILE = 'data.csv'
SYMBOL_FILE = 'allsymbols.csv'
FAIL_RECORDS_FILE = 'fail.csv'


def num2symbl(x):
    """
    convert code from 6 digitals to 6 digitals with .SS or .SZ sufix
    the first one used by tushare
    the latter used by yahoo
    :param x:
    str, such as 600109
    :return:
    str, such as 600109.SS
    """
    if x.startswith('60'):
        x = x+'.SS'
    else:
        x = x+'.SZ'
    return x


def symbl2num(x):
    """
    remove the suffix of the code
    :param x:
    str, such as 600109.SS
    :return:
    str, such as 600109
    """
    if len(x) == 9:
        return x[:6]
    else:
        print("symbol format error!!!!!!!!!!!   ", x)
    return None


def init_data_set(input_file):
    start = timeit.default_timer()
    try:
        data = pd.read_csv(input_file, dtype=CODE_DTYPE)
    except FileNotFoundError:
        print(input_file + ' doesn\'t exist ')
        return None
    data = __clean_data(data)
    if 'date' in data:
        data.date = data.date.astype('str')
        data.date = data.date.apply(lambda x: x.replace(' 00:00:00.000000', ''))
        data.date = pd.to_datetime(data.date)
    end = timeit.default_timer()
    print('********************initDataSet takes ' + str(end - start) + 's')
    return data


def __clean_data(data):
    if 'Unnamed: 0' in data:
        data = data.drop('Unnamed: 0', axis=1)
    if 'Unnamed: 0.1' in data:
        data = data.drop('Unnamed: 0.1', axis=1)
    if 'Unnamed: 1' in data:
        data = data.drop('Unnamed: 1', axis=1)
    if 'code.1' in data:
        data.drop('code.1', axis=1, inplace=True)
    if 'index' in data:
        data.drop('index', axis=1, inplace=True)
    data.drop_duplicates(inplace=True)
    return data