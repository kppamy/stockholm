#!/usr/bin/env python
# coding=utf-8
DATEFORMAT = '%Y-%m-%d'
ohlc_head = ['date', 'open', 'high', 'low', 'close', 'volume', 'adj_close', 'symbol']
# date head used by tushare : get_hists, get_hist_data, basic daily ticket date
tohlc_head = ['close', 'code', 'date', 'high', 'low', 'ma10', 'ma20', 'ma5', 'open', 'p_change',
                      'price_change', 'turnover', 'v_ma10', 'v_ma20', 'v_ma5', 'volume']
syminfo_head = ['industry_code', 'industry', 'code', 'name', 'area', 'concept_code', 'concept_name']
finance_head = ['symbol', 'name', 'industry', 'area', 'pe', 'outstanding', 'totals', 'totalassets', 'liquidassets',
                'fixedassets', 'reserved', 'reservedpershare', 'esp', 'bvps', 'pb', 'timetomarket', 'undp', 'perundp',
                'rev', 'profit', 'gpr', 'npr', 'holders']
min_head = ['code', 'name']
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