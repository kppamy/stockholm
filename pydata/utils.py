#!/usr/bin/env python
# coding=utf-8

import re
from pconst import *
import numpy as np
import pconst


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
        return x


def init_data_set(input_file):
    start = timeit.default_timer()
    try:
        data = pd.read_csv(input_file, dtype=CODE_DTYPE)
    except FileNotFoundError:
        print(input_file + ' doesn\'t exist ')
        return None
    data = clean_data(data)
    end = timeit.default_timer()
    print('********************initDataSet takes ' + str(end - start) + 's')
    return data


def remove_incomplete_symbols(data):
    KEY = 'code'
    HEAD = [KEY, 'close']
    ts = data.loc[:, HEAD]
    tsc = ts.groupby(KEY).count()
    db = tsc.describe()
    maj = db.loc['25%', 'close']
    mid = db.loc['50%', 'close']
    if (mid - maj)/mid > 0.7:
        good_symbols = tsc[tsc['close'] > maj].index
    else:
        good_symbols = tsc[tsc['close'] >= maj].index
    return data.loc[data.code.isin(good_symbols)]


def get_last_query_date():
    data = init_data_set(BASIC_DATA_FILE)
    pa = data.date.sort_values()
    last = pa.values[-1]
    last = str(last)[:10]
    print("latest query date: " + last)
    return last


def clean_data_files(path='.', start='', end='', pattern='', mode ='range'):
    '''
    Merge all files is between start and end
    :param start: short date str
    :param end: short date str
    :return:
    '''
    import os
    files = [f for f in os.listdir(path) if re.match(r'(^crawl)|(^yahoo)', f)]
    ff = []
    if mode == 'match':
        if pattern == '':
            ff = files
        else:
            ff = [f for f in files if f.find(pattern) != -1]
    elif mode == 'range':
        cps = 'crawl' + start
        cpe = 'crawl' + end
        yps = 'yahoo' + start
        ype = 'yahoo' + end
        ff = [f for f in files if ((f > cps) & (f < cpe)) | ((f > yps) & (f < ype))]
    data = pd.DataFrame()
    for f in ff:
        res = init_data_set(path + '/' + f)
        data = data.append(res)
    if len(data) > 0:
        data = data.drop_duplicates()
        new = persist_data(data, path)
        for f in ff:
            if not new.endswith(f):
                print('remove file ', f)
                os.remove(path + '/'+f)


def persist_data(data, path):
    if 'date' not in data:
        print('!!!!! input data frame doesn\'t contain \'date\'')
        return
    dts = data.date.drop_duplicates()
    dts = dts.sort_values()
    start = dts.iloc[0].strftime('%Y%m%d')
    end = dts.iloc[-1].strftime('%Y%m%d')
    if path.startswith('.'):
        path = path[2:]
    path = pconst.SOURCE_DIR + path + '/' + 'yahoo' + start + '_' + end + '.csv'
    data.to_csv(path)
    print('create new  file: ', path)
    return path


def get_last_work_day(day):
    """
    :param day:  target date, str type
    :return: str
    the last work day of input day
    """
    if day:
        anchor = datetime.strptime(day, DATEFORMAT)
    now = anchor.isoweekday()
    if now > 4:
        off = now - 5
        target = anchor - DateOffset(months=0, days=off)
        return target.strftime(DATEFORMAT)
    else:
        return day


def get_workdays(start, end):
    '''
    get work days between start and end
    :param start: date str YYYY-MM-DD
    :param end: date str YYYY-MM-DD
    :return:
    '''
    return (datetime.strptime(end, DATEFORMAT) - datetime.strptime(start, DATEFORMAT)).days


def clean_data(data):
    if len(data) == 0:
        print(" there is no data to clean !!!!!!!!!!!")
        return
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
    if 'date' in data:
        data.date = data.date.astype('str')
        data.date = data.date.apply(lambda x: x.replace(' 00:00:00.000000', ''))
        data.date = pd.to_datetime(data.date)
    data = data.fillna(method='ffill')
    res = data[(data.close != 0) & (data.open != 0)]
    return res


def complete_code(code):
    """
    find the one doesn't have 6 digits code:
    df[df.code.str.contains(re.compile(r'[0-9]{6}')) == 0 ].code.drop_duplicates()
    """
    length = len(code)
    tmp = ''
    if length < 6:
        more = 6-length
        tmp = '0' * more + code
        return tmp
    elif length == 6:
        return code
    else:
        print('code length big than 6, attention!!!')


def read_scrapy_json(file, data=None):

    """
    :param file:  str, json file name
    :param data: dataframe
    :return:
    """
    import json
    with open(file, 'r') as f:
        datastore = json.load(f)
    if data is None:
        data = pd.DataFrame()
    for item in datastore:
        quote = pd.DataFrame.from_dict(item)
        data = data.append(quote)
    return data


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


def reverse_bool_array(data):
    """
    :param data: type bool array
    :return:
    """
    return data.apply(lambda x: not x)


def to_dtype(data, key='volume', dtype=int):
    """
    Assign the give column in dataframe to specified data type
    :param data:  dataframe
    :param key: column
    :param dtype: datatype
    :return:
    """
    if (key not in data) or (data[key].dtype.type != np.object_):
        return
    regex = re.compile(r'[^0-9\.]')
    data[key] = data[key].str.replace(',', '')
    cond = data[key].str.match(regex)
    if len(cond[cond == True]) == 0:
        data[key] = data[key].astype(dtype)
        # print(key + ' matches , ignore')
        return
    delt = cond[cond == True]
    data[key].loc[delt.index] = 0
    data[key] = data[key].astype(dtype)


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


def unixTimestamp_trans(time):
    """
    Transfter unix timestamp to human readable time str
    such as from 1531896891 to '2018-07-18 14:54:51'
    :param time: int
    :return: str
    """
    va = datetime.datetime.fromtimestamp(time)
    value = va.strftime('%Y-%m-%d %H:%M:%S')
    return value


def backup_files(file):
    import os
    if os.path.isfile(file):
        os.rename(file, file + '.bak')
        print('backup file: ', file)



# read_scrapy_json('yahoo.json')

# ba=init_data_set('basic20180621_20190628_notverygood.csv')
# ts = remove_incomplete_symbols(ba)
# print(ts)

# clean_data_files(mode='match')
# merge_data_files('/Users/chenay/pyt/FinanceReportAnalysis/', 'finance_statement', dtype={'SECCODE':str})
