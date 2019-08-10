#!/usr/bin/python3


"""
Created on Wed Aug  7 16:20:40 2019

@author: mateng

Notation: this program is used to extract daily trading data for each stock 

"""


##### import the necessary packages
import tushare as ts

import pandas as pd
#import numpy as np
#import math
#import copy


##### set the connection with the database Tushare
TOKEN = 'dfb6e9f4f9a3db86c59a3a0f680a9bdc46ed1b5adbf1e354c7faa761'   ## Jason's token
ts.set_token(TOKEN)
pro = ts.pro_api()


##### set the constant variables
index_name = '399300.SZ'            ## input the name of the index:
                                    ## 399300.SZ or 000905.SH
                                    
startdate = '20020101'              ## set the start date of the data 
enddate = '20190806'                ## set the end date of the data                         

fields_daily_basic ='ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pb,ps,total_share,float_share,free_share,total_mv,circ_mv'


##### extract all the stock codes in the HS300 index and ZZ500 index
indexdata = pd.read_table('./output/' + index_name+ '_weight.txt', header='infer', encoding=None, delim_whitespace=True, index_col=0)

"""
index500 = pd.read_table('E:\\Practice in SCC\\Composite_python\\output\\000905.SH_weight.txt', 
                              header='infer', encoding=None, delim_whitespace=True, index_col=0)

indexdata = pd.concat([index300, index500], axis=0, join='inner')
"""

sec_codes = list(set(indexdata.con_code))
sec_codes.sort()

#df_now = pro.daily(trade_date = enddate)
#df_now = df_now.sort_values(by = 'ts_code',axis = 0,ascending = True)
#df_now.index = [i for i in range(df_now.shape[0])]

#sec_codes = df_now.ts_code


##### extract all the daily trading data for each stock
tradingdata = pd.DataFrame()


for i in range(len(sec_codes)):
    ## extract the daily trading data
    df_daily = pro.daily(ts_code = sec_codes[i], start_date = startdate, end_date = enddate)
    
    #df_daily_qfq = ts.pro_bar(ts_code=sec_codes[i], adj='qfq', start_date = startdate, end_date = enddate)
    #df_daily_qfq.columns = [x + '_qfq' for x in df_daily_qfq.columns]
    
    ## extract the hfq daily trading data
    df_daily_hfq = ts.pro_bar(ts_code=sec_codes[i], adj='hfq', start_date = startdate, end_date = enddate)
    df_daily_hfq.columns = [x + '_hfq' for x in df_daily_hfq.columns]
    
    ## extract the daily basic data
    df_daily_basic = pro.daily_basic(ts_code=sec_codes[i], start_date = startdate, end_date = enddate, fields=fields_daily_basic)
    
    ## merge the above trading data
    df = df_daily.merge(df_daily_hfq, how='inner', left_on=["ts_code", 'trade_date'], right_on=["ts_code_hfq", 'trade_date_hfq']) 
    
    df = df.merge(df_daily_basic, how='inner', left_on=["ts_code", 'trade_date'], right_on=["ts_code", 'trade_date']) 
    
    if df.shape[0] == 0:
        print(sec_codes[i] + 'has no index weight')
    else:
        print(i)
        if tradingdata.empty:
            tradingdata = df
        else:
            tradingdata = pd.concat([tradingdata, df], axis=0, join='inner')



##### output daily trading data for each stock   
tradingdata.to_csv('./output/' + index_name + '_tradingdata.txt', sep='\t',index=False)
 
    
    

    





