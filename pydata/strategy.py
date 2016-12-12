#!/usr/bin/env python
# coding=utf-8
import timeit
import io
import os
import csv
import re
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
def __init__(args):
    ## flag of if need to reload all stock data
    self.reload_data = args.reload_data
    ## type of output file json/csv or both
    self.output_type = args.output_type
    ## charset of output file utf-8/gbk
    self.charset = args.charset
    ## portfolio testing date range(# of days)
    #self.test_date_range = args.test_date_range
    ## stock data loading start date(e.g. 2014-09-14)
    self.start_date = args.start_date
    ## stock data loading end date
    self.end_date = args.end_date
    ## portfolio generating target date
    self.target_date = args.target_date
    ## thread number
    self.thread = args.thread
    ## data file store path
    if(args.store_path =='USER_HOME/tmp/pydata_export'):
        self.export_folder = os.path.expanduser('~') +'/tmp/pydata_export'
    else:
        self.export_folder = args.store_path
        ## for getting quote symbols
        self.all_quotes_url ='http://money.finance.sina.com.cn/d/api/openapi_proxy.php'
        ## for loading quote data
        self.yql_url ='http://query.yahooapis.com/v1/public/yql'
        ## export file name

def mark_single_quote(quote):
    df=quote
    df['mark']=0
    df.loc[(df.v_change>0) & (df.p_change>0),'mark']=2
    df.loc[(df.v_change<0) & (df.p_change>0),'mark']=1
    df.loc[(df.v_change<0) & (df.p_change<0),'mark']=-1
    df.loc[(df.v_change>0) & (df.p_change<0),'mark']=-2

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
    return res

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

def away51Top(data,n=3):
    tops=top_industry(data,n)
    r51=away51(data,'2016-12-02')
    res=r51.merge(tops,on='Symbol')
    res=res[['Date','Symbol','Name','Industry_Name','ma51','mark_y']]
    #print("top "+str(n)+" industry and far away from the 51 MA:\n",res.head())
    res=res.sort('Industry_Name')
    return res

def away51(m51,date):
    r51=m51[(( m51.Close > (m51.ma51*1.1))|(m51.Close < (m51.ma51*0.9)))&(m51.Date == date)]
    return r51

def run():
    data=pd.read_csv(OUTPUT_DATA_FILE)
    #basics_cal(data)
    #mark_all_down(data)
    #data.to_csv(OUTPUT_DATA_FILE)
    data=data.drop('Unnamed: 0',axis=1)
    data=data.drop('Unnamed: 0.1',axis=1)
    out=away51Top(data)
    print(out)
run()
