#!/usr/bin/env python
# coding=utf-8
import timeit
import pandas as pd

def mark_single_quote(quote):
    df=quote
    df['mark']=0
    df.loc[(df.v_change>0) & (df.p_change>0), 'mark']=2
    df.loc[(df.v_change<0) & (df.p_change>0), 'mark']=1
    df.loc[(df.v_change<0) & (df.p_change<0), 'mark']=-1
    df.loc[(df.v_change>0) & (df.p_change<0), 'mark']=-2

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

def mark_all_down( all_quotes):
    start = timeit.default_timer()
    st=all_quotes
    st.columns=['name', 'Symbol', 'Adj_Close', 'Close', 'Date','High','Low', 'Open', 'symbol', 'Volume']
    st=st.drop('symbol',axis=1)
    symbols=st[['name', 'Symbol']]
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
    industrys=data.industry_name.drop_duplicates()
    marks=data.groupby(['Symbol','industry_name'])['mark'].sum()
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
    #st.columns=['name', 'Symbol', 'Adj_Close', 'Close', 'Date','High','Low', 'Open', 'symbol', 'Volume']
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
    all_marks.to_csv('tmp.csv')
    print("process is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")
    return all_marks

def away51Top(data,n=3):
    tops=top_industry(data,n)
    r51=away51(data,'2016-08-12')
    res=r51.merge(tops,on='Symbol')
    res=res[['Date', 'Symbol', 'Name','industry_name','ma51', 'mark_y']]
    #print("top "+str(n)+" industry and far away from the 51 MA:\n",res.head())
    res=res.sort('industry_name')
    return res

def away51(m51,date):
    r51=m51[(( m51.Close > (m51.ma51*1.1))|(m51.Close < (m51.ma51*0.9)))&(m51.Date == date)]
    return r51

def run():
    #basics_cal(all_quotes)
    #mark_all_down(all_quotes)
    data=pd.read_csv('data.csv')
    data=data.drop('Unnamed: 0',axis=1)
    data=data.drop('Unnamed: 0.1',axis=1)
    out=away51Top(data)
    #print(out)

#run()
