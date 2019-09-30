# -*- coding: utf-8 -*-
"""
Created on  Sep 21

@author: Dawei Hang
"""

import os
import datetime
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

dir1='/work/dwhang/input/stock/sql/sse/daily/'
dir2='/work/dwhang/input/stock/sql/sse/dividend/'
dir3='zipline_csv/'

def zipline_csv_format(filecode,split_format=True):
    data=pd.read_csv(dir1+filecode)
    data['dividend']=0
    data['split']=1
    if split_format:
        divid_data=pd.read_csv(dir2+filecode,dtype={'ex_date':str})
        divid_data=divid_data[divid_data.div_proc=='实施']
        split=divid_data.loc[divid_data.stk_div>0,['stk_div','ex_date']]
        divid=divid_data.loc[divid_data.cash_div>0,['cash_div','ex_date']]
        split.stk_div=split.stk_div+1
        split.ex_date=split.ex_date.map(lambda x: datetime.strptime(x, '%Y%m%d').strftime('%Y-%m-%d'))
        divid.ex_date=divid.ex_date.map(lambda x: datetime.strptime(x, '%Y%m%d').strftime('%Y-%m-%d'))
        col_value=['open','high','low','close']

        for ex_date,div in zip(split.ex_date,split.stk_div):
            data.loc[data.date<ex_date,col_value]=data.loc[data.date<ex_date,col_value]/div
            data.loc[data.date<ex_date,'volume']=data.loc[data.date<ex_date,'volume']*div
            data.loc[data.date==ex_date,'split']=div

        for ex_date,dividend in zip(divid.ex_date,divid.cash_div):
            data.loc[data.date==ex_date,'dividend']=dividend
        
    data=data.drop('adjusted',axis=1)
    return(data)

def exception_write(error_list):
    """
    写入异常信息到日志
    """
    f = open('log.txt','a')
    for file in error_list:
        line="%s\n" % (file)
        f.write(line)
    f.close()

            
if __name__ == "__main__":
    data_file=set(os.listdir(dir1))
    split_file=set(os.listdir(dir2))
    for filecode in set(data_file-split_file):
        data=pd.read_csv(dir1+filecode)
        data['dividend']=0
        data['split']=1
        data=data.drop('adjusted',axis=1)
        temp.to_csv(dir3+filecode)
    filecodeerror=list()
    for filecode in set(data_file&split_file):
        try:
            temp=zipline_csv_format(filecode)
            temp.to_csv(dir3+filecode)
        except Exception as e:
            print(filecode)
            filecodeerror.append(filecode)
    exception_write(filecodeerror)
            
    