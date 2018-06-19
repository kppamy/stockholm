import csv
from datetime import datetime
import pandas as pd
import re
import numpy as np

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
    if len(x) > 6:
        return x
    if x.startswith('60'):
        x = x+'.SS'
    else:
        x = x+'.SZ'
    return x


def check_date_arg(value, arg_name=None):
    if value:
        try:
            if len(value) != 8:
                raise ValueError
            datetime.strptime(value, '%Y%m%d')
        except ValueError:
            raise ValueError("Option '%s' must be in YYYYMMDD format, input is '%s'" % (arg_name, value))


def parse_limit_arg(value):
    if value:
        tokens = value.split(',')
        try:
            if len(tokens) != 2:
                raise ValueError
            return int(tokens[0]), int(tokens[1])
        except ValueError:
            raise ValueError("Option 'limit' must be in START,COUNT format, input is '%s'" % value)
    return 0, None


def load_symbols(file_path):
    symbols = pd.Series()
    data = pd.read_csv(file_path, dtype={'code': 'str'})
    if 'code' in data:
        symbols = data.code
        symbols = symbols.apply(num2symbl)
    return symbols.tolist()


def parse_csv(file_like):
    reader = csv.reader(file_like)
    headers = reader.next()
    for row in reader:
        item = {}
        for i, value in enumerate(row):
            header = headers[i]
            item[header] = value
        yield item


def reverse_bool_array(data):
    """
    :param data: type bool array
    :return:
    """
    return data.apply(lambda x: not x)


def to_numstr(data, key='volume'):
    """
    :param data: DataFrame
    :param key:
    :return:
    """
    import re
    regex = re.compile(r'[0-9]')
    cond = data[key].str.match(regex)
    data[key][reverse_bool_array(cond)] = '0'


def to_dtype(data, key='volume', dtype=int):
    if (key not in data) or (data[key].dtype.type != np.object_):
        return
    regex = re.compile(r'[^0-9\.]')
    data[key] = data[key].str.replace(',', '')
    cond = data[key].str.match(regex)
    if len(cond[cond == True]) == 0:
        data[key] = data[key].astype(dtype)
        print(key + 'matches , ignore')
        return
    delt = cond[cond == True]
    data[key].loc[delt.index] = 0
    data[key] = data[key].astype(dtype)


def normal_yahoo_data(data):
    to_dtype(data, 'open', float)
    to_dtype(data, 'close', float)
    to_dtype(data, 'adj_close', float)
    to_dtype(data, 'high', float)
    to_dtype(data, 'low', float)
    to_dtype(data, 'volume', int)



def find_first_ohlc(items):
    reg = r'[a-z A-Z]{3} [0-9]{2}, [0-9]{4}'
    # print('Dec 20, 2017'.find()
    if len(items) == 0:
        print('crawl nothing ')
        return []
    first = items[items.str.contains(reg)]
    if len(first) > 0:
        findex = first.index[0]
        filter = items[findex:]
    else:
        print('can\'t find pattern match Date, ignore')
        filter = []
    return filter


def drop_row(items, key):
    """
    Drop the row of Series contains certain keyword
    :param items: pd.Series
    :param key: str
    :return:
    """
    trash = items[items.str.contains(key)]
    if trash is not None:
        items.drop(trash.index, inplace=True)
        if ((trash.index - 1) > 0).all():
            items.drop(trash.index - 1, inplace=True)


def parse_date_fromstr(date_str):
    if date_str:
        date = datetime.strptime(date_str, '%Y%m%d')
        return date
    return None

