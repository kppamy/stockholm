#!/usr/bin/env python
# coding=utf-8
import timeit
import io
import os
import csv
import re
import option
#from pymongo import MongoClient
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from liteDB import *
import pandas as pd
import numpy as np

DATEFORMAT='%Y-%m-%d'
DATA_HEAD=['Date','Open','High','Low','Close','Volume','Adj_Close','Symbol']
DATA_HEAD_ALL=[['Industry_Code', 'Industry_Name', 'Code', 'Name', 'Area','Concept_Code', 'Concept_Name']]
INPUT_DATA_FILE='query.csv'
OUTPUT_DATA_FILE='data.csv'

def mark_single_quote(quote):
    df=quote
    df['mark']=0
    df.loc[(df.v_change>0) & (df.p_change>0),'mark']=2
    df.loc[(df.v_change<0) & (df.p_change>0),'mark']=1
    df.loc[(df.v_change<0) & (df.p_change<0),'mark']=-1
    df.loc[(df.v_change>0) & (df.p_change<0),'mark']=-2
    
def group_cal(df):
    df.set_index('Date',inplace=True)
    df['v_change']=df['Volume'].pct_change()
    df['p_change']=df['Close'].pct_change()
    mark_single_quote(df)
    df['ma51']=df['Close'].rolling(51).mean()
    df['v6']=df['Volume'].rolling(6).mean()
    df['p_chg_aggr']=df['p_change'].rolling(3).sum()
    df.fillna(method='ffill',inplace=True)
    return df

def group_process():
    start=timeit.default_timer()
    d1=initDataSet(OUTPUT_DATA_FILE)
    d1.reset_index(inplace=True)
    q=pd.read_csv('crawl.csv')
    q.drop_duplicates(inplace=True)
    data=pd.concat((d1,q),ignore_index=True)
    tmp=data.groupby('Symbol').apply(group_cal)
    tmp.to_csv('group.csv')
    end=timeit.default_timer()
    print("basical group compute takes "+str(round(end-start))+"s ")

def get_allDB_data():
    data=pd.read_csv('tmp.csv')
    data=data.drop('Unnamed: 0',axis=1)
    ind=pd.read_csv('industry.csv')
    ind=ind.drop('Unnamed: 0',axis=1)
    symbols=data.Symbol
    data['code']=[x[:6] for x in symbols ]
    data.code=data.code.astype('int64')
    data1=pd.merge(data,ind,on='code',how='inner')
    return data1

def mark_all_down(all_quotes):
    start = timeit.default_timer()
    st=all_quotes
    st.columns=['Name','Symbol','Adj_Close','Close','Date','High','Low','Open','symbol','Volume']
    st=st.drop('symbol',axis=1)
    symbols=st[['Name','Symbol']]
    symbols.drop_duplicates(inplace=True)
    print('all symbols : \n',symbols.head())
    count=0
    for i in symbols[:4]:
        count=count+1
        print("process ",i)
        df=st[st.Symbol == i]
        mark_single_quote(df)
        if(count==1):
            all_marks=df
        else:
            all_marks=all_marks.append(df)
    print("mark data",all_marks)
    pd.DataFrame.to_csv(all_marks,'allmarks.csv')
    print("export is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")

def group_mean(df,column='Volume',n=6):
    return df[column].rolling(n).mean()

def find_special(day,df=None):
    if df is None:
       df=pd.DataFrame.from_csv(OUTPUT_DATA_FILE)
    df.reset_index(inplace=True)
    df.drop_duplicates(inplace=True)
    col=[u'Symbol', u'Name',u'Industry_Name',u'Date']
    ab=df[(df.p_change>0.7) | (df.p_change< -0.7)][col]
    ab['reason']='+_0.7'
    vh=df[df.Volume > (df.v6 * 2)][col]  
    vh['reason']='volume doubles'
    ah=df[(df.p_chg_aggr> 0.1)][col]
    ah['reason']='aggregate change more than +10%'
    ah2=df[(df.p_chg_aggr < -0.1)][col]
    ah2['reason']='aggregate change more than -10%'
    res=pd.concat((ab,vh,ah,ah2),ignore_index=True)
    #res.set_index('Date',inplace=True)
    res.to_csv('special.csv')
    print("what's special today:\n")
    today=res[res.Date == day]
    top=pd.read_csv('top.csv')
    if 'Unnamed: 0' in today:
        today.drop('Unnamed: 0',axis=1,inplace=True)
    top.drop('Unnamed: 0',axis=1,inplace=True)
    today.drop_duplicates(inplace=True)
    top.drop_duplicates(inplace=True)
    spe=pd.merge(today,top,on=['Symbol'])
    print(spe)

def append_average(df, col='Volume',win=6):
    cols=['Symbol',col]
    sle=df[cols]
    v6=sle.groupby('Symbol').apply(group_mean(sle[col],column=col,n=win))
    v6=v6.reset_index()
    v6=v6.set_index('level_1')
    v6.drop('Symbol',axis=1,inplace=True)
    res=pd.concat([df,v6],axis=1)
    res.drop(['Unnamed: 0.1','Unnamed: 0.1.1'],axis=1,inplace=True)
    return res

def ma_cal(df):
    prc=df.Close
    df['ma5']=prc.rolling(5).mean()
    df['ma10']=prc.rolling(10).mean()
    df['ma20']=prc.rolling(20).mean()
    df['ma51']=prc.rolling(51).mean()

def top_industry(data,n=3):
    industrys=data.Industry_Name.drop_duplicates()
    industrys=industrys.dropna()
    marks=data.groupby(['Symbol','Industry_Name'])['mark'].sum()
    res=pd.DataFrame(columns=['Symbol','mark'])
    for x in industrys:
        mark_ind=marks[:,x]
        mark_sort=mark_ind.sort_values()
        tmp=mark_sort[-n:]
        print(x)
        print("\n########################top "+ str(n)+ " of ",(x)," :\n",tmp)
        res=res.append(tmp.reset_index())
    res.to_csv('top.csv')
    return res

def get_industry_data(type='industry'):
    df=initDataSet(OUTPUT_DATA_FILE)
    df.reset_index(inplace=True)
    df['code']=df.Symbol.apply(lambda x: x[:6])
    df.set_index('code',inplace=True)
    dh=df.Symbol
    dic=dh.to_dict()
    df.reset_index(inplace=True)
    cpt=pd.read_csv('industry_detail.csv',dtype='str')
    if type == 'concept':
        cpt=pd.read_csv('concept_detail.csv',dtype='str')
    cpt['Symbol']=cpt.code.apply(lambda x:symbolConvert(x,dic))
    cpt.drop(['code'],inplace=True,axis=1)
    cpt.drop(['Unnamed: 0'],inplace=True,axis=1)
    df.drop('code',axis=1,inplace=True)
    data=pd.merge(df,cpt,on='Symbol')
    if 'Industry_Name' in data:
        data.drop('Industry_Name',axis=1,inplace=True)
    data['Industry_Name']=data.c_name
    data.drop('c_name',axis=1,inplace=True)
    data.to_csv(OUTPUT_DATA_FILE)

def symbolConvert(x,dic):
    if x in dic:
        return dic[x]
    else:
        return x

def rank_industry(symbol,industry=None):
    """
    get an industry mark rank through a stock
    symbol:string, the symbol of the code
    """
    data2=pd.DataFrame.from_csv(OUTPUT_DATA_FILE)
    data2.reset_index(inplace=True)
    gb=data2[['Symbol','Name','Industry_Name','mark']].groupby(['Symbol','Name','Industry_Name']).sum()
    gb=gb.reset_index()
    ind=industry
    if industry == None or industry == '':
        ind=gb[gb.Symbol==symbol]['Industry_Name']
        res=gb[gb.Industry_Name == ind.values[0]]
        industry=ind.values[0]
    else:
        res=gb[gb.Industry_Name == industry]
    res=res.sort_values(by='mark',ascending=False)
    res.reindex()
    res['ranks']=res['mark'].rank(method='first')
    res.to_csv('rank'+industry+'.csv')
    print(res)

def basics_cal(all_quotes):
    start = timeit.default_timer()
    st=all_quotes
    #st=st.drop('Unnamed: 0',axis=1)
    #st.columns=['name','Symbol','Adj_Close','Close','Date','High','Low','Open','symbol','Volume']
    #st=st.drop('symbol',axis=1)
    symbols=st.Symbol
    sym=symbols.drop_duplicates()
    print('all symbols : \n',symbols.head())
    count=0
    #from IPython import embed
    #embed()
    for i in sym:
        count=count+1
        print("process ",i)
        df=st.loc[st.Symbol == i]
        v=df.Volume
        df['v_change']=v.pct_change()
        prc=df.Close
        df['p_change']=prc.pct_change()
        ma_cal(df)
        mark_single_quote(df)
        if(count==1):
            all_marks=df
        else:
            all_marks=all_marks.append(df)
    print("basics data process",all_quotes.head())
    all_marks.to_csv('all_marks.csv')
    print("process is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
    return all_marks

def away51Top(data,bench,n=3):
    tops=top_industry(data,n)
    r51=away51(data,bench)
    res=r51.merge(tops,on='Symbol')
    res=res[['Date','Symbol','Name','Industry_Name','ma51','mark_y']]
    #print("top "+str(n)+" industry and far away from the 51 MA:\n",res.head())
    res=res.sort('Industry_Name')
    return res

def away51(m51,date):
    r51=m51[(( m51.Close > (m51.ma51*1.1))|(m51.Close < (m51.ma51*0.9)))&(m51.Date == date)]
    return r51

def run():
    args=option.parser.parse_args()
    data=pd.DataFrame([],columns=DATA_HEAD_ALL)
    if args.methods == 'basic':
        print('*****basic data processing *********')
        #data=initDataSet(INPUT_DATA_FILE)
        #basics_cal(data)
        #mark_all_down(data)
        group_process()
    elif args.methods == 'away51':
        data=initDataSet(OUTPUT_DATA_FILE)
        out=away51Top(data,args.end_date)
        out.to_csv('away51.csv')
        print(out)
    elif args.methods=='rank':
        rank_industry(args.symbol,args.industry)
    elif args.methods == 'special':
        find_special(args.end_date)
    elif args.methods == 'foundation':
        get_industry_data()

def initDataSet(file):
    data=pd.read_csv(file)
    if 'Unnamed: 0' in data:
        data=data.drop('Unnamed: 0',axis=1)
    if 'Unnamed: 0.1' in data:
        data=data.drop('Unnamed: 0.1',axis=1)
    if 'Symbol.1' in data:
        data.drop('Symbol.1',axis=1,inplace=True)
    if 'index' in data:
        data.drop('index',axis=1,inplace=True)
    data.Date=pd.to_datetime(data.Date)
    return data

if __name__ == '__main__':
    run()
