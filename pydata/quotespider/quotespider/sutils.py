import csv
from datetime import datetime
from utils import *


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


def normal_yahoo_data(data):
    to_dtype(data, 'open', float)
    to_dtype(data, 'close', float)
    to_dtype(data, 'adj_close', float)
    to_dtype(data, 'high', float)
    to_dtype(data, 'low', float)
    to_dtype(data, 'volume', int)


def to_tushare(data):
    normal_yahoo_data(data)
    data.code = data.code.apply(symbl2num)
    data = data.sort_values(['code', 'date'])
    data = data.replace(0, np.NAN)
    data = data.fillna(method='ffill')
    if 'adj_close' in data:
        data.drop('adj_close', axis=1, inplace=True)
    trash = data[data[['volume', 'open', 'close', 'high', 'low']].isnull().all(axis=1)]
    data = data.drop(trash.index)
    return data


def find_first_ohlc(items):
    # reg = r'[a-z A-Z]{3} [0-9]{2}, [0-9]{4}'
    # print('Dec 20, 2017'.find()
    if len(items) == 0:
        print('crawl nothing ')
        return []
    num_reg = re.compile(r'[^0-9\.,]')
    judge = items.str.contains(num_reg)
    pattern = judge.rolling(6).sum()
    first = pattern[pattern == 1]
    if len(first) > 0:
        findex = (first.index - 6)[0]
        filter = items[findex:]
    else:
        print('can\'t find pattern match Date, ignore')
        filter = []
    return filter


def drop_trash(items):
    drop_row(items, 'Dividend')
    drop_row(items, 'Stock Split')
    drop_row(items, 'td class')


def parse_date_fromstr(date_str):
    if date_str:
        date = datetime.strptime(date_str, '%Y%m%d')
        return date
    return None