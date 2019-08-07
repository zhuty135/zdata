#!/usr/bin/python3

"""
Created on Wed Aug  7 14:04:22 2019

@author: mateng

Notation: this program is used to extract the weight data of index.

"""


##### import the necessary packages
import pandas as pd
#import numpy as np
#import math
#import copy

import tushare as ts

""" 
def get_token():
    import configparser
    cp = configparser.ConfigParser()
    cp.read('../factors/config/databasic.cfg')
    sect = 'tushare'
    token = eval(cp.get(sect,'TOKEN'))
    return token

mytoken = get_token()#'dfb6e9f4f9a3db86c59a3a0f680a9bdc46ed1b5adbf1e354c7faa761'

ts.set_token(mytoken)
pro = ts.pro_api(mytoken)

"""
##### set the connection with the database Tushare
TOKEN = 'dfb6e9f4f9a3db86c59a3a0f680a9bdc46ed1b5adbf1e354c7faa761'   ## Jason's token
ts.set_token(TOKEN)
pro = ts.pro_api()



##### set the constant variables
index_name = '399300.SZ'            ## input the name of the index:
                                    ## 399300.SZ or 000905.SH
startdate = '1/1/2002'              ## set the start date of the data 
enddate = '12/31/2019'              ## set the end date of the data                         


##### construct the date dataframe
mDate = pd.DataFrame({'start': pd.date_range(start = startdate, end = enddate, freq = 'MS'),
                      'end': pd.date_range(start = startdate, end = enddate, freq = 'M')})

for i in range(mDate.shape[0]):
    str_start = str(mDate.loc[i,'start'])
    str_start = str_start[:10]
    #str_start.replace("-", "")
    mDate.loc[i,'start'] = str_start.replace("-", "")
    
    str_end = str(mDate.loc[i,'end'])
    str_end = str_end[:10]
    #str_end.replace("-", "")
    mDate.loc[i,'end'] = str_end.replace("-", "")


##### extract the weight data of index
idx_weight = pd.DataFrame()

for i in range(mDate.shape[0]):
    df = pro.index_weight(index_code=index_name, start_date = mDate.loc[mDate.shape[0]-1-i,'start'], end_date = mDate.loc[mDate.shape[0]-1-i,'end'])
    if df.shape[0] == 0:
        print(mDate.loc[mDate.shape[0]-1-i,'start']+'has no index weight')
    else:
        print(i)
        if idx_weight.empty:
            idx_weight = df
        else:
            idx_weight = pd.concat([idx_weight, df], axis=0, join='inner')

  
##### output the weight data of index   
idx_weight.to_csv('./output/' + index_name+ '_weight.txt', sep='\t',index=False)
    
#mmdate = list(idx_weight.trade_date.unique())





















