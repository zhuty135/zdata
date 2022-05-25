#!/usr/bin/python3
import pandas as pd
import numpy as np
import sys
import os
import pwd
import re
uname = pwd.getpwuid(os.getuid()).pw_name
sys.path.append('/work/'+uname+ '/project/zlib/')
from zutils import get_prev_business_date, get_business_date_list



def fill_data(df): 
    dt_fmt='%Y-%m-%d'
    sd = df.index[0].strftime(dt_fmt)
    ed = df.index[-1].strftime(dt_fmt)
    bd_list = get_business_date_list(fmt=dt_fmt,caltype='XSHG')
    #short_bd_list = pd.to_datetime(bd_list[(bd_list >= sd) & (bd_list <= ed)]).tz_localize('UTC')
    short_bd_list = pd.to_datetime(bd_list).tz_localize('UTC')

    print('jzcheck',sd,ed,df.iloc[-10:,])
    print(short_bd_list)
    df = df.reindex(short_bd_list, method='ffill').fillna(method='bfill')


    df.sort_index(inplace=True)
    try:
        #df.index.df.drop_duplicates(inplace=True)
        df = df[~df.index.duplicated()]

        #df = df.drop_duplicates()
    except Exception as err:
        print('jzerror:', str(err))
    print('jzcheck1',df.iloc[0:10,])
    print('jzcheck2',df.iloc[-10:,])
    df.sort_index(inplace=True)

    return(df)
 
def from_csv(path,opath):
    print(path)
    tmpdf = pd.read_csv(
        path,
        parse_dates=['Time Period'], # parse_dates=[0],
        #na_values=['ND'],  # Presumably this stands for "No Data".
        index_col='Time Period'
    ).tz_localize('UTC')
    #tmpdf['Time Period'] = pd.to_datetime(tmpdf['Time Period'], utc=True)
    #tmpdf.set_index('Time Period',inplace=True)
    newdf = fill_data(tmpdf)
    newdf.index.name = 'Time Period'
    newdf.to_csv(opath,index=True,header=True)
    print('output to:',opath)

    return newdf

f = '/work/jzhu/input/tsy/treasury-curve.csv'
of = '/work/jzhu/data/pol/' + f
from_csv(f,of)
