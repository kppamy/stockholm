import csv
from datetime import datetime
import pandas as pd

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
