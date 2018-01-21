#!/usr/bin/env python
# coding = utf-8
import option
from liteDB import *
import pandas as pd
import tushare as ts
from const import *
import numpy as np


def __mark_single_quote(quote):
    df = quote
    df['mark'] = 0
    df.loc[(df.v_change > 0) & (df.price_change > 0), 'mark'] = 2
    df.loc[(df.v_change < 0) & (df.price_change > 0), 'mark'] = 1
    df.loc[(df.v_change < 0) & (df.price_change < 0), 'mark'] = -1
    df.loc[(df.v_change > 0) & (df.price_change < 0), 'mark'] = -2
    return df


def __group_cal(df):
    """
    badic caculations on datafrmae
    :param df:
    pd.DataFrame
    :return:
    pd.DataFrame
    """
    df.sort_values(by='date', inplace=True)
    df['v_change'] = df['volume'].pct_change()
    df['ma51'] = df['close'].rolling(51).mean()
    df['v6'] = df['volume'].rolling(6).mean()
    # df['price_change'] = df['close'].pct_change()
    # df['ma5'] = df['close'].rolling(5).mean()
    # df['ma20' ] = df['close'].rolling(20).mean()
    df['ma30'] = df['close'].rolling(30).mean()
    df['price_chg_aggr'] = df['price_change'].rolling(3).sum()
    df['max'] = df.close.max()
    df.fillna(method='ffill', inplace=True)
    return df


def __group_process(data, new=None):
    start = timeit.default_timer()
    d1 = data
    if new is not None:
        data = pd.concat((d1, new), ignore_index=True)
    data = data.drop_duplicates()
    data.to_csv(BASIC_DATA_FILE)
    tmp = data.groupby(BASCIC_KEY).apply(__group_cal)
    tmp = __mark_single_quote(tmp)
    end = timeit.default_timer()
    print("basic group compute takes " + str(round(end - start)) + "s ")
    return tmp


def __read_csv(file):
    q = pd.read_csv(file)
    q = q.drop('Unnamed: 0', axis=1)
    q.drop_duplicates(inplace=True)
    if 'date' in q:
        q.date = pd.to_datetime(q.date)
    return q


def get_allDB_data():
    data = pd.read_csv('tmp.csv')
    data = data.drop('Unnamed: 0', axis=1)
    ind = pd.read_csv('industry.csv')
    ind = ind.drop('Unnamed: 0', axis=1)
    symbols = data.symbol
    data['code'] = [x[:6] for x in symbols]
    data.code = data.code.astype('int64')
    data1 = pd.merge(data, ind, on='code', how='inner')
    return data1


def __mark_all_down(all_quotes):
    start = timeit.default_timer()
    st = all_quotes
    st.columns = OHLC_HEAD
    st = st.drop('code', axis=1)
    symbols = st[MIN_HEAD]
    symbols.drop_duplicates(inplace=True)
    print('all symbols : \n', symbols.head())
    count = 0
    for i in symbols[:4]:
        count = count + 1
        print("process ", i)
        df = st[st.symbol == i]
        __mark_single_quote(df)
        if count == 1:
            all_marks = df
        else:
            all_marks = all_marks.append(df)
    print("mark data", all_marks)
    pd.DataFrame.to_csv(all_marks, 'allmarks.csv')
    print("export is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")


def __group_mean(df, column='volume', n=6):
    return df[column].rolling(n).mean()


def find_special(data, bench):
    if data is None:
        data = __init_data_set(OUTPUT_DATA_FILE)
    df = data
    df.drop_duplicates(inplace=True)
    col = [u'code', u'date']
    ab = df[(df.price_change > 0.08) | (df.price_change < -0.08)][col]
    ab['reason'] = '+_8%'
    vh = df[df.volume > (df.v6 * 3)][col]
    vh['reason'] = 'volume doubles'
    ah = df[(df.price_chg_aggr > 0.15)][col]
    ah['reason'] = 'aggregate change more than +15%'
    ah2 = df[(df.price_chg_aggr < -0.1)][col]
    ah2['reason'] = 'aggregate change more than -15%'
    # ah4 = df[((df['max']-df.close)/df['max'])< 0.1)][col]
    # ah4['reason'] = 'no more than 10% away from max'
    res = pd.concat((ab, vh, ah, ah2), ignore_index=True)
    # res.set_index('date',inplace = True)
    today = res[res.date == bench]
    top = top_industry(data, 'industry', None)
    res = today.merge(top, on='code')
    today = res
    print("*********************************what's special today:\n", today)
    return today


def find_high(data, bench, atr=3):
    df = data
    col = [u'code', u'date']
    ah3 = df[((df['max'] - df.close) / df['max']) < (atr / 100)][col]
    ah3['reason'] = 'no more than ' + str(atr) + '% away from max'
    top = top_industry(data, 'industry', None)
    today = ah3[ah3.date == bench]
    res = today.merge(top, on='code')
    print('*************************climb**************************')
    print(res)
    return res


def find_long_short(data, bench):
    df = data
    long = __is_long(bench, df)
    short = __is_short(bench, df)
    res = pd.concat((long, short), ignore_index=True)
    print(res)
    return res


def __special_top(special):
    top = __read_csv('top.csv')
    spe = pd.merge(special, top, on=['code'])
    print(spe)


def __is_long(day, data):
    df = data[data['date'] == day]
    lg = df[(df.close > df.ma5) & (df.ma5 > df.ma20) & (df.ma20 > df.ma30) & (df.ma30 > df.ma51)]
    if len(lg) > 0:
        lg.loc[:, 'reason'] = 'long array'
        return lg
    return None


def __is_short(day, data):
    df = data[data['date'] == day]
    lg = df[(df.close < df.ma5) & (df.ma5 < df.ma20) & (df.ma20 < df.ma30) & (df.ma30 < df.ma51)]
    if len(lg) > 0:
        lg.loc[:, 'reason'] = 'short array'
        return lg
    return None


def __append_average(df, col='volume', win=6):
    cols = ['code', col]
    sle = df[cols]
    v6 = sle.groupby('code').apply(__group_mean(sle[col], column=col, n=win))
    v6 = v6.reset_index()
    v6 = v6.set_index('level_1')
    v6.drop('code', axis=1, inplace=True)
    res = pd.concat([df, v6], axis=1)
    if 'Unnamed: 0' in res:
        res.drop(['Unnamed: 0.1', 'Unnamed: 0.1.1'], axis=1, inplace=True)
    return res


def __ma_cal(df):
    prc = df.close
    # ma5,ma10,ma20 are provided by tushare on default
    # df['ma5'] = prc.rolling(5).mean()
    # df['ma10'] = prc.rolling(10).mean()
    # df['ma20'] = prc.rolling(20).mean()
    df['ma51'] = prc.rolling(51).mean()


def top_industry(data, key='industry', value=None, num=3):
    """
    Show top N symbols in each industry/area/concept
    All rank symbols in certain industry/area/concept
    :param data:
    type: Pd.DataFrame
    :param key:
    type: str
    can be one of industry/area/concept
    :param value:
    type: str
    the value of the selected key industry/area/concept
    default is None, show top N in each industry/area/concept
    :param num:
    type: int
    when set to None, rank symbols in certain industry/area/concept
    :return:
    """
    s3 = __append_list(MIN_HEAD, [key])
    s4 = __append_list(s3, ['mark'])
    select = pd.DataFrame()
    if value is None:
        select = data
    else:
        select = data[data[key] == value]
    marks = select[s4].groupby(s3)['mark'].sum()
    marks = marks.reset_index()
    res = pd.DataFrame()
    if num is None:
        res = marks.sort_values(by='mark', ascending=0)
    else:
        res = marks.groupby(key, group_keys=False).apply(__sort_mark, num)
    print('==============rank ' + value + '=================')
    print(res)
    res.to_csv('top' + '_' + value + '.csv')
    return res


def __sort_mark(df, num=3):
    res = df.sort_values(by='mark', ascending=0)
    return res[:num]


def get_industry_data(df, source='Local', key='industry'):
    if source == 'Local':
        finance = __init_data_set(FINANCE_FILE)
    else:
        finance = ts.get_stock_basics()
        finance.reset_index(inplace=True)
        finance.columns = FINANCE_HEAD
        finance[MIN_HEAD].to_csv(SYMBOL_FILE)
        finance.to_csv(FINANCE_FILE)
    mcolumns = __append_list(MIN_HEAD, [key])
    if key in df.columns:
        mkeys = mcolumns
    else:
        mkeys = MIN_HEAD
    data = pd.merge(df, finance[mcolumns], how='left', on=mkeys)
    len1 = len(df[BASCIC_KEY].drop_duplicates())
    len2 = len(data[BASCIC_KEY].drop_duplicates())
    if len1 != len2:
        print("!!!!!!!!!!!!!!!!!!!!data might gets lost !!!!!!!!!!!!!!  before merge: " + str(len1)
              + " after merge: " + str(len2))
        return None
    else:
        return data


def __get_symbol_dict(df):
    df.reset_index(inplace=True)
    df['code'] = df.symbol.apply(lambda x: x[:6])
    df = df.set_index('code')
    dh = df.symbol
    dic = dh.to_dict()
    df.reset_index(inplace=True)
    return dic


def __symbol_convert(x, dic):
    if x in dic:
        return dic[x]
    else:
        return x


def rank_industry(data, industry_value=None, symbol=None, key='industry'):
    """
    get an industry mark rank through a stock
    symbol:string, the symbol of the code
    """
    if industry_value is not None and industry_value != '':
        select = industry_value
        top_industry(data, key, select, None)
    else:
        industry_name = data[data[BASCIC_KEY] == symbol][key].drop_duplicates()
        values = industry_name.values
        if len(values) == 0:
            print('There is no data found for symbol ' + symbol)
            return
        elif isinstance(values, list) or isinstance(values, pd.Series) \
                or isinstance(values, set) or isinstance(values, np.ndarray):
            for value in values:
                top_industry(data, key, value, None)


def basics_cal(all_quotes):
    start = timeit.default_timer()
    st = all_quotes
    # st = st.drop('Unnamed: 0',axis = 1)
    # st.columns = ['name','code','Adj_close','close','date','High','Low','Open','code','volume']
    # st = st.drop('code',axis = 1)
    symbols = st.symbol
    sym = symbols.drop_duplicates()
    print('all symbols : \n', symbols.head())
    count = 0
    # from IPython import embed
    # embed()
    for i in sym:
        count = count + 1
        print("process ", i)
        df = st.loc[st.symbol == i]
        v = df.volume
        df['v_change'] = v.pct_change()
        prc = df.close
        df['price_change'] = prc.pct_change()
        __ma_cal(df)
        __mark_single_quote(df)
        if count == 1:
            all_marks = df
        else:
            all_marks = all_marks.append(df)
    print("basics data process", all_quotes.head())
    all_marks.to_csv('all_marks.csv')
    print("process is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
    return all_marks


def away51_top(data, bench, num=3, key='industry'):
    tops = top_industry(data, key, None, num)
    r51 = __away51(data, bench)
    mkeys = __append_list(MIN_HEAD, [key])
    res = r51.merge(tops, on=mkeys)
    res = res[['date', 'close', 'code', 'name', key, 'ma51', 'mark_y']]
    print(res)
    # print("top "+str(n)+" industry and far away from the 51 MA:\n",res.head())
    # res = res.sort('Industry')
    return res


def __append_list(list1, list2):
    return pd.concat([pd.Series(list1), pd.Series(list2)]).tolist()


def __away51(m51, date):
    r51 = m51[(m51.close < (m51.ma51 * 0.9)) & (m51.date == date)]
    # r51 = m51[((m51.close > (m51.ma51*1.1)) | (m51.close < (m51.ma51*0.9))) & (m51.date == date)]
    return r51


def __init_data_set(input_file):
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


def update_concept(key='concept'):
    con = ts.get_concept_classified()
    # con.columns = __append_list(MIN_HEAD, [key])
    finance_data = __init_data_set(FINANCE_FILE)
    if key in finance_data:
        finance_data.drop(key, axis=1, inplace=True)
        finance_data.drop_duplicates(inplace=True)
    res = pd.merge(finance_data, con, on=MIN_HEAD)
    res.to_csv(FINANCE_FILE)
    return res


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


def set_test_args(args, method, end_date, symbol, industry, category):
    args.methods = method
    args.symbol = symbol
    args.category = category
    # args.industry = industry
    args.end_date = end_date
    return args


def run():
    args = option.parser.parse_args()
    crawl_file_name = 'crawl' + args.start_date.replace('-', '') + '_' + args.end_date.replace('-', '') + '.csv'
    data = pd.DataFrame()
    args = set_test_args(args, 'rank', '2018-01-11', '601009', '信托重仓', 'industry')
    if args.methods == 'basic':
        print('*****basic data processing *********')
        data = __init_data_set(BASIC_DATA_FILE)
        new = __init_data_set(crawl_file_name)
        # basics_cal(data)
        # __mark_all_down(data)
        data = __group_process(data, new)
    elif args.methods == 'foundation':
        data = __init_data_set(OUTPUT_DATA_FILE)
        data = get_industry_data(data, key=args.category)
    elif args.methods == 'finance':
        update_concept()
        return
    elif args.methods == 'report':
        data = __init_data_set(BASIC_DATA_FILE)
        new = __init_data_set(crawl_file_name)
        data = __group_process(data, new)
        out = away51_top(data, args.end_date, key=args.category)
        find_special(data, args.end_date)
    if data is not None and len(data) != 0:
        data.to_csv(OUTPUT_DATA_FILE)
        return
    data = __init_data_set(OUTPUT_DATA_FILE)
    if args.methods == 'away51':
        out = away51_top(data, args.end_date, key=args.category)
        out.to_csv('away51top.csv')
    elif args.methods == 'rank':
        rank_industry(data, args.industry, args.symbol, args.category)
    elif args.methods == 'special':
        find_special(data, args.end_date)
    elif args.methods == 'high':
        find_high(data, args.end_date)
        find_long_short(data, args.end_date)


if __name__ == '__main__':
    run()
