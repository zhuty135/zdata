#!/usr/local/anaconda3/bin/python3.9

import pandas as pd
import numpy as np
import os
rawtikcers=['SW857411.PO','000827.SH','SW857372.PO','SW857373.PO','SW857355.PO','SW850854.PO','830933.NQ','SW857321.PO','SW857641.PO','SW851523.PO','SW857371.PO','SW850831.PO','SW857451.PO','3993.HK','SW801750.PO','3800.HK','SW850817.PO','SW801765.PO']
codes=['000300.SH','000905.SH','000852.SH','000016.SH','159915.SZ']

df=pd.DataFrame()

extcodes = codes.update(rawtikcers)
print(extcodes)
for code in codes:
    path_file='/work/shared/moredata/'+code+".csv"
#    path_file="C:\\MyDocs\\\data\\\moredata\\"+code+".csv"#
    x=pd.read_csv(path_file)
    x=x[x['adjusted']>0]
    x=x.set_index('date')
    x=x['adjusted']
    x=x/x.shift(1)-1
    df[code]=x
    
df=df[df.index>='2011-08-03']
x=df.loc[:,['000300.SH','000905.SH','000852.SH']]
x.insert(0,'constant',1)
x=np.array(x)
y=df.loc[:,'000988.SH']
y=np.array(y)
reg= np.linalg.lstsq(x, y,rcond=None)
coef=reg[0]
coef=pd.DataFrame(data=coef,index=['constant']+['000300.SH','000905.SH','000852.SH'],columns=['coef'])
print(coef)
