#!/usr/bin/env python
# coding=utf-8
import timeit
import pandas as pd
class Strategy(object):

    def mark_single_quote(self,quote):
            df=quote
            df.loc[(df.Volume>0)&(df.Volume >df.shift(1).Volume) & (df.Adj_Close > df.shift(1).Adj_Close),'mark']=2
            df.loc[(df.Volume>0)&(df.Volume >df.shift(1).Volume) & (df.Adj_Close < df.shift(1).Adj_Close),'mark']=-2
            df.loc[(df.Volume>0)&(df.Volume >df.shift(1).Volume) & (df.Adj_Close == df.shift(1).Adj_Close),'mark']=1
            df.loc[(df.Volume>0)&(df.Volume == df.shift(1).Volume) & (df.Adj_Close >df.shift(1).Adj_Close),'mark']=1
            df.loc[(df.Volume>0)&(df.Volume == df.shift(1).Volume) & (df.Adj_Close < df.shift(1).Adj_Close),'mark']= -1
            df.loc[(df.Volume>0)&(df.Volume == df.shift(1).Volume) & (df.Adj_Close == df.shift(1).Adj_Close),'mark']= 0
            df.loc[(df.Volume>0)&(df.Volume < df.shift(1).Volume) & (df.Adj_Close > df.shift(1).Adj_Close),'mark']=1
            df.loc[(df.Volume>0)&(df.Volume < df.shift(1).Volume) & (df.Adj_Close < df.shift(1).Adj_Close),'mark']=-1
            df.loc[(df.Volume>0)&(df.Volume < df.shift(1).Volume) & (df.Adj_Close == df.shift(1).Adj_Close),'mark']=-1
            
    def mark_all_down(self, all_quotes):
        start = timeit.default_timer()
        st=all_quotes
        st.columns=['Name', 'Symbol', 'Adj_Close', 'Close', 'Date','High','Low', 'Open', 'symbol', 'Volume']
        st=st.drop('symbol',axis=1)
        symbols=st.Symbol.unique()
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
