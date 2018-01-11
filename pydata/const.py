#!/usr/bin/env python
# coding=utf-8
DATEFORMAT = '%Y-%m-%d'
DATA_HEAD = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj_Close','Symbol']
DATA_HEAD_ALL = [['Industry_Code', 'Industry', 'Code', 'Name', 'Area', 'Concept_Code', 'Concept_Name']]
DATA_HEAD_SHORT = ['Symbol', 'Name']
BASIC_DATA_FILE = 'basic.csv'
FINANCE_FILE = 'finance.csv'
OUTPUT_DATA_FILE = 'data.csv'
SYMBOL_FILE = 'allsymbols.csv'
FAIL_RECORDS_FILE='fail.csv'
INVALID_CODE = '000000'

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
        x=x+'.SS'
    else:
        x=x+'.SZ'
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
    return INVALID_CODE