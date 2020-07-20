import pandas as pd
import numpy as np
from datetime import datetime as dt

import qutils.futils as fu


QFINANCE = 'qfinance.csv'
ACCU_PRICE_CHANGE= 'accu_chg'

def get_cash_ability():
    '''
    :return:
    '''
    ts = pd.read_csv('/Users/chenay/pyt/pydata/pydata/financial_overview.csv',index_col=0,dtype={'SECCODE':str},low_memory=False)
    sub = ts[['SECCODE','SECNAME','year','净利润','长期借款','股东权益合计','经营活动产生的现金流量净额','应付债券款',
            '短期借款','负债和所有者权益(或股东权益)总计','负债合计','股东权益合计','报表日期']]
    data = sub[sub['净利润'] > 0]
    data['所有者权益(或股东权益)合计'] = data['负债和所有者权益(或股东权益)总计'] - data['负债合计']
    data['应付债券款'] = data['应付债券款'].replace(np.NaN,0)
    data['有息负债'] = data['应付债券款']+data['长期借款']+data['短期借款']
    data['debt_rights'] = data['有息负债']/data['所有者权益(或股东权益)合计']
    data['netprofits2cash'] = data['经营活动产生的现金流量净额']/data['净利润']
    data['购建固定资产、无形资产和其他长期资产支付的现金'] = ts['购建固定资产、无形资产和其他长期资产支付的现金'].replace(np.NaN,0)
    data['free_cash'] = data['经营活动产生的现金流量净额']-data['购建固定资产、无形资产和其他长期资产支付的现金']
    res = data[['SECCODE', 'netprofits2cash', 'free_cash','debt_rights']]
    res.columns = ['code', 'netprofits2cash', 'free_cash','debt_rights']
    return res


def get_accu_interests():
    di = pd.read_csv('/Users/chenay/pyt/eastmoney_spider/data/dividend_data.csv', dtype={'Code': str})
    di.XJFH.replace('-', 0, inplace=True)
    di.XJFH = di.XJFH.astype(float)
    di.TotalEquity.replace('-', 0, inplace=True)
    di.TotalEquity = di.TotalEquity.astype(float)
    sub = di[['Code', 'Name', 'XJFH', 'AllocationPlan', 'TotalEquity', 'SGBL', 'SZZBL', 'ZGBL', 'CQCXR']]
    new = sub.groupby('Code').apply(get_interests)
    res = new[['Code', 'qinterests']].groupby('Code').sum()
    res.reset_index(inplace=True)
    res.columns = ['code', 'qinterests']
    # res.to_csv('accumulates_interests.csv')
    return res


def get_interests(sf):
    sf['qinterests'] = (sf['TotalEquity'] / 10) * sf['XJFH']
    return sf


def get_acc_profites(year=(dt.today().year - 1)):
    an = pd.read_csv('annual2000_2019.csv', dtype={'code': str, 'net_profits': float}, index_col=0)
    an.roe = an.roe.replace(np.NaN, 0)
    an19 = an[an.period == (year*100 + 4)]
    res = an[['code', 'net_profits']].groupby('code').sum()
    res.reset_index(inplace=True)
    res['roe'] = an19['roe']
    res['netprof_new'] = an19['net_profits']
    return res


def get_interests_profits():
    interests = get_accu_interests()
    profits = get_acc_profites()
    res = profits.merge(interests, on='code')
    res['accu_interests_profits'] = res.qinterests / (res.net_profits * 10000)
    return res[['code', 'accu_interests_profits', 'roe', 'netprof_new']]


def get_basis():
    fd = pd.read_csv('fundamental.csv', dtype={'code': str})
    res = fd[['code', 'timetomarket', 'pe', 'pb']].drop_duplicates()
    return res


def get_pledge_ration(ratio):
    plg = pd.read_csv('/Users/chenay/pyt/eastmoney_spider/data/gpzyhgmx_20200419_20200425.csv', dtype={'证券代码': str})
    plg[plg['质押比例（%）'] < ratio].head()
    plg.columns
    sub = plg[['统计日期', '证券代码', '质押比例（%）']]
    sub.columns = ['querydate', 'code', 'pledge_ratio']
    res = sub[sub.pledge_ratio < ratio]
    return res


def get_basic_finance():
    cash = get_cash_ability()
    generosity = get_interests_profits()
    cand = cash.merge(generosity, on='code')
    age = get_basis()
    res = cand.merge(age, on='code')
    plg = get_pledge_ration(100)
    res = res.merge(plg, on='code')
    res.to_csv(QFINANCE)
    return res


def select_cow(data):
    # roe > 10 %
    # 经营净现金流／净利润 > 0.6
    # 有息负债／股东权益 < 50 %
    # 股权质押 < 30 %
    # 自由现金流 > 0
    # 累计分红／累计净利润 > 30 %
    # 上市时间 > 3年
    target = data[(data.roe > 10)
                  & (data.netprofits2cash > 0.6)
                  & (data.debt_rights < 0.5)
                  & (data.free_cash > 0)
                  & (data.accu_interests_profits > 0.3)
                  & (data.timetomarket < 20170630)
                  & (data.timetomarket > 0)
                  & (data.pledge_ratio > 30)]
    target.to_csv('targets.csv')
    return target


def select_overview(update=False):
    import os
    if os.path.isfile(QFINANCE) and not update:
        res = pd.read_csv(QFINANCE, index_col=0, dtype={'code': str})
    else:
        res = get_basic_finance()
    if update:
        select_cow(res)
    res.to_csv('qselect.csv')
    return res


def get_performance(data):
    if data is None:
        tt = pd.read_csv('basic.csv', index_col=0, dtype={'code': str})
    else:
        tt = data
    if 'date' in tt.columns:
        td = tt.set_index('date')
    else:
        td = tt
    td.sort_index(inplace=True)
    res = td.groupby('code').apply(get_chg)
    res['date'] = td.index[-1]
    res.reset_index(inplace=True)
    res.to_csv('general_performance.csv')
    return res


def get_chg(quote):
    quote['accu_chg'] = (quote['close'].iloc[-1] - quote['close'].iloc[0]) / quote['close'].iloc[0]
    quote['accu_chg'] = (quote['close'].iloc[-1] - quote['close'].iloc[0]) / quote['close'].iloc[0]
    return quote[['accu_chg']].iloc[-1]

# res=get_accu_interests()
# print(res.head())

# get_acc_profites()

# select()

