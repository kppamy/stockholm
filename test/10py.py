#!/usr/bin/env python
# coding=utf-8
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
s=pd.Series([1,3,5,np.nan,6,8])
print(s)
dates=pd.date_range('20160701',periods=6)
print(dates)
df=pd.DataFrame(np.random.randn(6,4),index=dates,columns=list('ABCD'))
print("df...................\n",df)
#print(df.dtyps)
date2=pd.date_range('20160801',periods=3)
df2=pd.DataFrame(np.random.randn(3,4),index=date2,columns=list('ABCD'))
print("df2..................\n",df2)
df3=df.append(df2)
print("df+df2..................\n",df3)
dc={'one':[1,2],'two':[3,4]}
df4=pd.DataFrame(dc)
print(df4)
