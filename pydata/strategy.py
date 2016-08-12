#!/usr/bin/env python
# coding=utf-8
import timeit
import pandas as pd
class Strategy(object):

    def mark_single_quote(self,quote):
        df=quote
        df['mark']=0
        df.loc[(df.v_change>0) & (df.p_change>0), 'mark']=2
        df.loc[(df.v_change<0) & (df.p_change>0), 'mark']=1
        df.loc[(df.v_change<0) & (df.p_change<0), 'mark']=-1
        df.loc[(df.v_change>0) & (df.p_change<0), 'mark']=-2

    def mark_all_down(self, all_quotes):
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
            self.mark_single_quote(df)
            if(count==1):
                self.all_marks=df
            else:
                self.all_marks=self.all_marks.append(df)
                print("mark data",self.all_marks)
                pd.DataFrame.to_csv(self.all_marks,'allmarks.csv')
                print("export is complete... time cost: " + str(round(timeit.default_timer() - start)) + "s" + "\n")

    def ma_cal(self,df):
        prc=df.Close
        df['ma5']=prc.rolling(5).mean()
        df['ma10']=prc.rolling(10).mean()
        df['ma20']=prc.rolling(20).mean()

    def basics_cal(self,all_quotes):
        start = timeit.default_timer()
        st=all_quotes
        st.columns=['name', 'Symbol', 'Adj_Close', 'Close', 'Date','High','Low', 'Open', 'symbol', 'Volume']
        st=st.drop('symbol',axis=1)
        symbols=st.Symbol
        symbols.drop_duplicates(inplace=True)
        print('all symbols : \n',symbols.head())
        symbols.index=range(len(symbols))
        count=0
        #from IPython import embed
        #embed()
        for i in range(4):
            count=count+1
            print("process ",)
            print (symbols[i])
            df=st.loc[st.Symbol == symbols[i]]
            v=df.Volume
            df['v_change']=v.pct_change()
            prc=df.Close
            df['p_change']=prc.pct_change()
            self.ma_cal(df)
            self.mark_single_quote(df)
            if(count==1):
                self.all_marks=df
            else:
                self.all_marks=self.all_marks.append(df)
        print("basics data process",all_quotes.head())
        self.all_marks.to_csv('tmp.csv')
    def run(self,all_quotes):
        self.basics_cal(all_quotes)
        #self.mark_all_down(all_quotes)
