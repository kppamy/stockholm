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
from pandas import DataFrame
import numpy as np
import tushare as ts
from const import *

def mark_single_quote(quote):
    df=quote
    df['mark']=0
    df.loc[(df.v_change>0) & (df.p_change>0),'mark']=2
    df.loc[(df.v_change<0) & (df.p_change>0),'mark']=1
    df.loc[(df.v_change<0) & (df.p_change<0),'mark']=-1
    df.loc[(df.v_change>0) & (df.p_change<0),'mark']=-2
    return df

def group_cal(df):
    df['v_change']=df['Volume'].pct_change()
    df['p_change']=df['Close'].pct_change()
    df['ma51']=df['Close'].rolling(51).mean()
    df['v6']=df['Volume'].rolling(6).mean()
    df['ma5']=df['Close'].rolling(5).mean()
    df['ma20']=df['Close'].rolling(20).mean()
    df['ma30']=df['Close'].rolling(30).mean()
    df['p_chg_aggr']=df['p_change'].rolling(3).sum()
    df['max']=df.Close.max()
    df.fillna(method='ffill',inplace=True)
    return df

def group_process(data):
    start=timeit.default_timer()
    d1=data
    #q=readCsv('crawl.csv')
    q=initDataSet('crawl.csv')
    data=pd.concat((d1,q),ignore_index=True)
    data=data.drop_duplicates()
    data.to_csv(BASIC_DATA_FILE)
    tmp=data.groupby('Symbol').apply(group_cal)
    tmp=mark_single_quote(tmp)
    end=timeit.default_timer()
    print("basical group compute takes "+str(round(end-start))+"s ")
    return tmp

def readCsv(file):
    q=pd.read_csv(file)
    q=q.drop('Unnamed: 0',axis=1)
    q.drop_duplicates(inplace=True)
    if 'Date' in q:
        q.Date=pd.to_datetime(q.Date)
    return q

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

def find_special(data,bench):
    if data is None:
       data=pd.DataFrame.from_csv(OUTPUT_DATA_FILE)
    df=data
    df=df.reset_index()
    df.drop_duplicates(inplace=True)
    col=[u'Symbol',u'Date']
    ab=df[(df.p_change>0.08) | (df.p_change< -0.08)][col]
    ab['reason']='+_8%'
    vh=df[df.Volume > (df.v6 * 3)][col]
    vh['reason']='volume doubles'
    ah=df[(df.p_chg_aggr> 0.15)][col]
    ah['reason']='aggregate change more than +15%'
    ah2=df[(df.p_chg_aggr < -0.1)][col]
    ah2['reason']='aggregate change more than -15%'
    #ah4=df[((df['max']-df.Close)/df['max'])< 0.1)][col]
    #ah4['reason']='no more than 10% away from max'
    res=pd.concat((ab,vh,ah,ah2),ignore_index=True)
    #res.set_index('Date',inplace=True)
    today=res[res.Date == bench]
    top=top_industry(data)
    res=today.merge(top,on='Symbol')
    today=res
    print("*********************************what's special today:\n",today)
    return today

def find_high(data,bench,atr=3):
    df=data
    col=[u'Symbol',u'Date']
    ah3=df[((df['max']-df.Close)/df['max'])< (atr/100)][col]
    ah3['reason']='no more than '+str(atr)+'% away from max'
    top=top_industry(data)
    today=ah3[ah3.Date == bench]
    res=today.merge(top,on='Symbol')
    print('*************************climb**************************')
    print(res)
    return res

def find_longShort(data,bench):
    df=data
    long=is_long(bench,df)
    short=is_short(bench,df)
    res=pd.concat((long,short),ignore_index=True)
    return res

def specialTop(special):
    top=readCsv('top.csv')
    spe=pd.merge(special,top,on=['Symbol'])
    print(spe)

def is_long(day,data):
    df=data[data['Date']==day]
    lg=df[(df.Close>df.ma5)&(df.ma5>df.ma20)&(df.ma20>df.ma30)&(df.ma30>df.ma51)]
    if len(lg) > 0:
        lg['reason']='long array'
        return lg
    return None

def is_short(day,data):
    df=data[data['Date']==day]
    lg=df[(df.Close<df.ma5)&(df.ma5<df.ma20)&(df.ma20<df.ma30)&(df.ma30<df.ma51)]
    if len(lg) > 0:
        lg['reason']='short array'
        return lg
    return None

def append_average(df, col='Volume',win=6):
    cols=['Symbol',col]
    sle=df[cols]
    v6=sle.groupby('Symbol').apply(group_mean(sle[col],column=col,n=win))
    v6=v6.reset_index()
    v6=v6.set_index('level_1')
    v6.drop('Symbol',axis=1,inplace=True)
    res=pd.concat([df,v6],axis=1)
    if 'Unnamed: 0' in res:
        res.drop(['Unnamed: 0.1','Unnamed: 0.1.1'],axis=1,inplace=True)
    return res

def ma_cal(df):
    prc=df.Close
    df['ma5']=prc.rolling(5).mean()
    df['ma10']=prc.rolling(10).mean()
    df['ma20']=prc.rolling(20).mean()
    df['ma51']=prc.rolling(51).mean()

def top_industry(bas,key='Industry',n=3):
    data=DataFrame()
    data=get_industry_data(bas,key,'Local')
    if key not in data:
        data=pd.readCsv(OUTPUT_DATA_FILE)
    marks=data[['Symbol','Name',key,'mark']].groupby(['Symbol','Name',key])['mark'].sum()
    marks=marks.reset_index()
    res=pd.DataFrame()
    res=marks.groupby(key,group_keys=False).apply(sortMark,num=n)
    #print(res)
    #industrys=data[key].drop_duplicates()
    #industrys=industrys.dropna()
    #for x in industrys:
    #    mark_ind=marks[:,x]
    #    mark_sort=mark_ind.sort_values()
    #    tmp=mark_sort[-n:]
    #    print("\n########################top "+ str(n)+ " of ",(x)," :\n",tmp)
    #    res=res.append(tmp.reset_index())
    #res.to_csv('top.csv')
    return res

def sortMark(df,num=3):
    res=df.sort_values(by='mark',ascending=0)
    return res[:num]

def get_industry_data(df, source='Online', key='Industry'):
    if source == 'Local':
        bas=pd.read_csv(FINANCE_FILE)
    else:
        bas=ts.get_stock_basics()
        bas.reset_index(inplace=True)
        bas.code=bas.code.apply(num2symbl)
        bas.columns=[['Symbol','Name', 'Industry', 'Area', 'pe', 'outstanding', 'totals','totalAssets', 'liquidAssets', 'fixedAssets', 'reserved','reservedPerShare', 'esp', 'bvps', 'pb', 'timeToMarket', 'undp','perundp', 'rev', 'profit', 'gpr', 'npr', 'holders']]
        bas[DATA_HEAD_SHORT].to_csv(SYMBOL_FILE)
        bas.to_csv(FINANCE_FILE)
    data=pd.merge(df,bas[['Symbol','Name',key]],on='Symbol')
    return data
    #cpt=DataFrame()
    #if type == 'concept':
    #    cpt=pd.read_csv('concept_detail.csv',dtype='str')
    #elif type == 'ind':
    #    cpt=pd.read_csv('industry_detail.csv',dtype='str')
    #cpt.drop(['Unnamed: 0'],inplace=True,axis=1)
    #dic=getSymbolDict(df)
    #cpt['Symbol']=cpt.code.apply(lambda x:symbolConvert(x,dic))
    #cpt.drop(['code'],inplace=True,axis=1)
    #df.drop('code',axis=1,inplace=True)
    #if 'Industry' in data:
    #    data.drop('Industry',axis=1,inplace=True)
    #data['Industry']=data.c_name
    #data.drop('c_name',axis=1,inplace=True)

def getSymbolDict(df):
    df.reset_index(inplace=True)
    df['code']=df.Symbol.apply(lambda x: x[:6])
    df=df.set_index('code')
    dh=df.Symbol
    dic=dh.to_dict()
    df.reset_index(inplace=True)
    return dic

def symbolConvert(x,dic):
    if x in dic:
        return dic[x]
    else:
        return x

def rank_industry(data,symbol,industry,key='Industry'):
    """
    get an industry mark rank through a stock
    symbol:string, the symbol of the code
    """
    #data=pd.DataFrame.from_csv(OUTPUT_DATA_FILE)
    data=get_industry_data(data,key,'Local')
    #data.reset_index(inplace=True)
    gb=data[['Symbol','Name',key,'mark']].groupby(['Symbol','Name',key]).sum()
    gb=gb.reset_index()
    ind=industry
    res=DataFrame()
    if industry == None or industry == '':
        ind=gb[gb.Symbol==symbol][key]
        res=gb[gb[key]== ind.values[0]]
        industry=ind.values[0]
    else:
        res=gb[gb[key]== industry]
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

def away51Top(data,bench,num=3,key='Industry'):
    tops=top_industry(data,key,n=num)
    r51=away51(data,bench)
    res=r51.merge(tops,on='Symbol')
    res=res[['Date','Close','Symbol','Name',key,'ma51','mark_y']]
    print(res)
    #print("top "+str(n)+" industry and far away from the 51 MA:\n",res.head())
    #res=res.sort('Industry')
    return res

def away51(m51,date):
    r51=m51[((m51.Close < (m51.ma51*0.9)))&(m51.Date == date)]
    #r51=m51[(( m51.Close > (m51.ma51*1.1))|(m51.Close < (m51.ma51*0.9)))&(m51.Date == date)]
    return r51

def run():
    args=option.parser.parse_args()
    data=DataFrame()
    if args.methods == 'basic':
        print('*****basic data processing *********')
        data=initDataSet(BASIC_DATA_FILE)
        #basics_cal(data)
        #mark_all_down(data)
        data=group_process(data)
        data.to_csv(OUTPUT_DATA_FILE)
        return
    elif args.methods == 'foundation':
        data=initDataSet(BASIC_DATA_FILE)
        data=get_industry_data(data)
        data.to_csv(OUTPUT_DATA_FILE)
        return
    elif args.methods == 'finance' :
         updateConcept()
         return
    elif args.methods == 'report':
        d0=initDataSet(BASIC_DATA_FILE)
        data=group_process(d0)
        out=away51Top(data,args.end_date)
        find_special(data,args.end_date)
        data.to_csv(OUTPUT_DATA_FILE)
        return
    data=initDataSet(OUTPUT_DATA_FILE)
    if args.methods == 'away51':
        out=away51Top(data,args.end_date)
        #out.to_csv('away51top.csv')
    elif args.methods=='rank':
        rank_industry(data,args.symbol,args.industry,args.category)
    elif args.methods == 'special':
        find_special(data,args.end_date)
    elif args.methods == 'high':
        find_high(data,args.end_date)
    data.to_csv(OUTPUT_DATA_FILE)

def initDataSet(inputFile):
    start=timeit.default_timer()
    data=pd.read_csv(inputFile)
    data=cleanData(data)
    if 'Date' in data :
        data.Date=data.Date.astype('str')
        data.Date=data.Date.apply(lambda x: x.replace(' 00:00:00.000000',''))
        data.Date=pd.to_datetime(data.Date)
    end=timeit.default_timer()
    print('********************initDataSet takes '+str(end-start)+'s')
    return data

def updateConcept(key='concept'):
    con=ts.get_concept_classified()
    con.columns=['code', 'Name', key]
    fi=initDataSet(FINANCE_FILE)
    if key in fi:
        fi.drop(key,axis=1,inplace=True)
        fi.drop_duplicates(inplace=True)
    res=pd.merge(fi,con[['Name', key]],on='Name')
    res.to_csv(FINANCE_FILE)
    return res

def cleanData(data):
    if 'Unnamed: 0' in data:
        data=data.drop('Unnamed: 0',axis=1)
    if 'Unnamed: 0.1' in data:
        data=data.drop('Unnamed: 0.1',axis=1)
    if 'Symbol.1' in data:
        data.drop('Symbol.1',axis=1,inplace=True)
    if 'index' in data:
        data.drop('index',axis=1,inplace=True)
    data.drop_duplicates(inplace=True)
    return data

if __name__ == '__main__':
    run()
