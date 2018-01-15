#!/usr/bin/env python
# coding = utf-8
import option
from liteDB import *
import pandas as pd
import tushare as ts
from const import *


def mark_single_quote(quote):
    df = quote
    df['mark'] = 0
    df.loc[(df.v_change > 0) & (df.price_change > 0), 'mark'] = 2
    df.loc[(df.v_change < 0) & (df.price_change > 0), 'mark'] = 1
    df.loc[(df.v_change < 0) & (df.price_change < 0), 'mark'] = -1
    df.loc[(df.v_change > 0) & (df.price_change < 0), 'mark'] = -2
    return df


def group_cal(df):
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
    # df['ma30'] = df['close'].rolling(30).mean()
    df['price_chg_aggr'] = df['price_change'].rolling(3).sum()
    df['max'] = df.close.max()
    df.fillna(method='ffill', inplace=True)
    return df


def group_process(data, new=None):
    start = timeit.default_timer()
    d1 = data
    if new is not None:
        data = pd.concat((d1, new), ignore_index=True)
    data = data.drop_duplicates()
    data.to_csv(BASIC_DATA_FILE)
    tmp = data.groupby('code').apply(group_cal)
    tmp = mark_single_quote(tmp)
    end = timeit.default_timer()
    print("basic group compute takes " + str(round(end - start)) + "s ")
    return tmp


def read_csv(file):
    q = pd.read_csv(file)
    q = q.drop('Unnamed: 0', axis=1)
    q.drop_duplicates(inplace=True)
    if 'Date' in q:
        q.Date = pd.to_datetime(q.Date)
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


def mark_all_down(all_quotes):
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
        mark_single_quote(df)
        if count == 1:
            all_marks = df
        else:
            all_marks = all_marks.append(df)
    print("mark data", all_marks)
    pd.DataFrame.to_csv(all_marks, 'allmarks.csv')
    print("export is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")


def group_mean(df, column='volume', n=6):
    return df[column].rolling(n).mean()


def find_special(data, bench):
    if data is None:
        data = pd.DataFrame.from_csv(OUTPUT_DATA_FILE)
    df = data
    df = df.reset_index()
    df.drop_duplicates(inplace=True)
    col = [u'code', u'Date']
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
    # res.set_index('Date',inplace = True)
    today = res[res.Date == bench]
    top = top_industry(data, 'industry', None)
    res = today.merge(top, on='code')
    today = res
    print("*********************************what's special today:\n", today)
    return today


def find_high(data, bench, atr=3):
    df = data
    col = [u'code', u'Date']
    ah3 = df[((df['max'] - df.close) / df['max']) < (atr / 100)][col]
    ah3['reason'] = 'no more than ' + str(atr) + '% away from max'
    top = top_industry(data, 'industry', None)
    today = ah3[ah3.Date == bench]
    res = today.merge(top, on='code')
    print('*************************climb**************************')
    print(res)
    return res


def find_long_short(data, bench):
    df = data
    long = is_long(bench, df)
    short = is_short(bench, df)
    res = pd.concat((long, short), ignore_index=True)
    return res


def special_top(special):
    top = read_csv('top.csv')
    spe = pd.merge(special, top, on=['code'])
    print(spe)


def is_long(day, data):
    df = data[data['Date'] == day]
    lg = df[(df.close > df.ma5) & (df.ma5 > df.ma20) & (df.ma20 > df.ma30) & (df.ma30 > df.ma51)]
    if len(lg) > 0:
        lg['reason'] = 'long array'
        return lg
    return None


def is_short(day, data):
    df = data[data['Date'] == day]
    lg = df[(df.close < df.ma5) & (df.ma5 < df.ma20) & (df.ma20 < df.ma30) & (df.ma30 < df.ma51)]
    if len(lg) > 0:
        lg['reason'] = 'short array'
        return lg
    return None


def append_average(df, col='volume', win=6):
    cols = ['code', col]
    sle = df[cols]
    v6 = sle.groupby('code').apply(group_mean(sle[col], column=col, n=win))
    v6 = v6.reset_index()
    v6 = v6.set_index('level_1')
    v6.drop('code', axis=1, inplace=True)
    res = pd.concat([df, v6], axis=1)
    if 'Unnamed: 0' in res:
        res.drop(['Unnamed: 0.1', 'Unnamed: 0.1.1'], axis=1, inplace=True)
    return res


def ma_cal(df):
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
    s1 = pd.Series(MIN_HEAD)
    s2 = pd.Series([key])
    s3 = pd.concat([s1, s2])
    s4 = pd.concat([s3, pd.Series('mark')])
    if value is None:
        marks = data[s4].groupby(s3.tolist())['mark'].sum()
    else:
        select = data[data[key] == value]
        marks = select[s4].groupby(s3.tolist())['mark'].sum()
    marks = marks.reset_index()
    res = pd.DataFrame()
    if num is None:
        res = marks.sort_values(by='mark', ascending=0)
    else:
        res = marks.groupby(key, group_keys=False).apply(sort_mark, num)
    res.to_csv('top.csv')
    return res


def sort_mark(df, num=3):
    res = df.sort_values(by='mark', ascending=0)
    return res[:num]


def get_industry_data(df, source='Local', key='industry'):
    if source == 'Local':
        bas = pd.read_csv(FINANCE_FILE)
    else:
        bas = ts.get_stock_basics()
        bas.reset_index(inplace=True)
        bas.columns = FINANCE_HEAD
        bas[MIN_HEAD].to_csv(SYMBOL_FILE)
        bas.to_csv(FINANCE_FILE)
    MIN_HEAD.append(key)
    data = pd.merge(df, bas[MIN_HEAD], on='code')
    return data
    # cpt = DataFrame()
    # if type   ==   'concept':
    #    cpt = pd.read_csv('concept_detail.csv',dtype = 'str')
    # elif type   ==   'ind':
    #    cpt = pd.read_csv('industry_detail.csv',dtype = 'str')
    # cpt.drop(['Unnamed: 0'],inplace = True,axis = 1)
    # dic = getsymbolDict(df)
    # cpt['code'] = cpt.code.apply(lambda x:symbolConvert(x,dic))
    # cpt.drop(['code'],inplace = True,axis = 1)
    # df.drop('code',axis = 1,inplace = True)
    # if 'Industry' in data:
    #    data.drop('Industry',axis = 1,inplace = True)
    # data['Industry'] = data.c_name
    # data.drop('c_name',axis = 1,inplace = True)


def get_symbol_dict(df):
    df.reset_index(inplace=True)
    df['code'] = df.symbol.apply(lambda x: x[:6])
    df = df.set_index('code')
    dh = df.symbol
    dic = dh.to_dict()
    df.reset_index(inplace=True)
    return dic


def symbol_convert(x, dic):
    if x in dic:
        return dic[x]
    else:
        return x


def rank_industry(data, industry_value, symbol=None, key='industry'):
    """
    get an industry mark rank through a stock
    symbol:string, the symbol of the code
    """
    select = None
    if industry_value is None or industry_value == '':
        industry_name = data[data[BASCIC_KEY] == symbol][key].drop_duplicates()
        values = industry_name.values
        if len(values) > 1:
            print('attention!!! ' + symbol + "belongs to multiple " + key + ' you need to update API : rank_industry')
        select = values[0]
    else:
        select = industry_value
    if select is None:
        print("Please identify name of symbol or industry for search")
    else:
        return top_industry(data, key, select, None)


def basics_cal(all_quotes):
    start = timeit.default_timer()
    st = all_quotes
    # st = st.drop('Unnamed: 0',axis = 1)
    # st.columns = ['name','code','Adj_close','close','Date','High','Low','Open','code','volume']
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
        ma_cal(df)
        mark_single_quote(df)
        if count == 1:
            all_marks = df
        else:
            all_marks = all_marks.append(df)
    print("basics data process", all_quotes.head())
    all_marks.to_csv('all_marks.csv')
    print("process is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
    return all_marks


def away51Top(data, bench, num=3, key='industry'):
    tops = top_industry(data, key, None, num)
    r51 = away51(data, bench)
    res = r51.merge(tops, on=['code', 'name', key])
    res = res[['date', 'close', 'code', 'name', key, 'ma51', 'mark_y']]
    print(res)
    # print("top "+str(n)+" industry and far away from the 51 MA:\n",res.head())
    # res = res.sort('Industry')
    return res


def away51(m51, date):
    r51 = m51[(m51.close < (m51.ma51 * 0.9)) & (m51.date == date)]
    # r51 = m51[((m51.close > (m51.ma51*1.1)) | (m51.close < (m51.ma51*0.9))) & (m51.date == date)]
    return r51


def init_data_set(input_file):
    start = timeit.default_timer()
    try:
        data = pd.read_csv(input_file, dtype={'code': 'str'})
    except FileNotFoundError:
        print(input_file + ' doesn\'t exist ')
        return None
    data = clean_data(data)
    if 'Date' in data:
        data.Date = data.Date.astype('str')
        data.Date = data.Date.apply(lambda x: x.replace(' 00:00:00.000000', ''))
        data.Date = pd.to_datetime(data.Date)
    end = timeit.default_timer()
    print('********************initDataSet takes ' + str(end - start) + 's')
    return data


def update_concept(key='concept'):
    con = ts.get_concept_classified()
    con.columns = ['code', 'Name', key]
    fi = init_data_set(FINANCE_FILE)
    if key in fi:
        fi.drop(key, axis=1, inplace=True)
        fi.drop_duplicates(inplace=True)
    res = pd.merge(fi, con[['Name', key]], on='Name')
    res.to_csv(FINANCE_FILE)
    return res


def clean_data(data):
    if 'Unnamed: 0' in data:
        data = data.drop('Unnamed: 0', axis=1)
    if 'Unnamed: 0.1' in data:
        data = data.drop('Unnamed: 0.1', axis=1)
    if 'symbol.1' in data:
        data.drop('symbol.1', axis=1, inplace=True)
    if 'index' in data:
        data.drop('index', axis=1, inplace=True)
    data.drop_duplicates(inplace=True)
    return data


def run():
    args = option.parser.parse_args()
    CRAWL_FILE_NAME = 'crawl' + args.start_date.replace('-', '') + '_' + args.end_date.replace('-', '') + '.csv'
    data = pd.DataFrame()
    if args.methods == 'basic':
        print('*****basic data processing *********')
        data = init_data_set(BASIC_DATA_FILE)
        new = init_data_set(CRAWL_FILE_NAME)
        # basics_cal(data)
        # mark_all_down(data)
        data = group_process(data, new)
        data.to_csv(OUTPUT_DATA_FILE)
        return
    elif args.methods == 'foundation':
        data = init_data_set(OUTPUT_DATA_FILE)
        data = get_industry_data(data, source='online')
        data.to_csv(OUTPUT_DATA_FILE)
        return
    elif args.methods == 'finance':
        update_concept()
        return
    elif args.methods == 'report':
        d0 = init_data_set(BASIC_DATA_FILE)
        data = group_process(d0)
        out = away51Top(data, args.end_date)
        find_special(data, args.end_date)
        data.to_csv(OUTPUT_DATA_FILE)
        return
    data = init_data_set(OUTPUT_DATA_FILE)
    if args.methods == 'away51':
        args.end_date = '2018-01-11'
        out = away51Top(data, args.end_date)
        out.to_csv('away51top.csv')
    elif args.methods == 'rank':
        res = rank_industry(data, args.industry, args.symbol, args.category)
        print(res)
    elif args.methods == 'special':
        find_special(data, args.end_date)
    elif args.methods == 'high':
        find_high(data, args.end_date)
    data.to_csv(OUTPUT_DATA_FILE)


if __name__ == '__main__':
    run()
