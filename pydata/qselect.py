import pandas as pd
import numpy as np
from datetime import datetime as dt

import qutils.futils as fu


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

def select():
    # roe > 10 %
    # 经营净现金流／净利润 > 0.6
    # 有息负债／股东权益 < 50 %
    # 股权质押 < 30 %
    # 自由现金流 > 0
    # 累计分红／累计净利润 > 30 %
    # 上市时间 > 3年
    cash = get_cash_ability()
    generosity = get_interests_profits()
    age = get_basis()
    cand = cash.merge(generosity, on='code')
    res = cand.merge(age, on='code')
    target = res[(res.roe > 10)
                 & (res.netprofits2cash > 0.6)
                 & (res.debt_rights < 0.5)
                 & (res.free_cash > 0)
                 & (res.accu_interests_profits > 0.3)
                 & (res.timetomarket < 20170630) & (res.timetomarket > 0)]
    target.to_csv('targets.csv')

# res=get_accu_interests()
# print(res.head())

# get_acc_profites()

select()