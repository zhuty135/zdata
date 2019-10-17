#!/usr/bin/python3

import pandas as pd 
import numpy as np 
import os
import re
import shutil
import time

from datetime import date, datetime, timedelta
from zipline.utils.calendars import get_calendar
import sys
import pwd


from sqlalchemy import create_engine #pymongo

from pprint import pprint
#mongo_client = pymongo.MongoClient("60.205.230.96", 27018)


#fut_dict = {'SHFE':'shf'}
fut_dict = {'CFFEX':'cfx','DCE':'dce','CZCE':'zce','SHFE':'shf','INE':'ine'}
opt_dict = {'SSE':'sse','DCE':'dce','CZCE':'zce','SHFE':'shf'}
fund_nav_dict = {'E':'e','O':'o'}
index_dict = {'CSI':'csi','SSE':'sse','SZSE':'szse'}
stock_dict = {'SSE':'sse','SZSE':'szse'}
def filter_fut_symb(s):
    skip_flag = False
    #if not re.match(r'^\w{2}[1-8]\d{1}.*\.SHF$',s):#ZN/RU
    if not re.match(r'^\w{2}[1-8]\d{1}.*\.\w{3}$',s):#ZN/RU
        skip_flag = True#continue
    return skip_flag

def filter_opt_symb(s):
    fs = s.split('.')
    skip_flag = False
    if  not re.match(r'.*\..*$',s):
        skip_flag = True
    elif fs[1] in ('ZCE',):
        if not re.match(r'SR\d0[159].*$',fs[0]) and not re.match(r'CF\d0[159].*$',fs[0]):# SR711C7100.ZCE.csv
            print('skip',fs[0])
            skip_flag = True#continue
    elif fs[1] in ('DCE',):
        if not re.match(r'M\d{2}0[159]\-.*$',fs[0]) and not re.match(r'C\d{2}0[159]\-.*$',fs[0]):# M2001-P-2500
            print('skip',fs[0])
            skip_flag = True#continue
    elif fs[1] in ('SHF',):
        if not re.match(r'RU\d{2}0[159].*$',fs[0]) and  not re.match(r'CU.*$',fs[0]):#CU1905C49000.SHF.csv
            print('skip',fs[0])
            skip_flag = True#continue
    return skip_flag


fs_list = ['daily_basic','fina_indicator','income','balancesheet','cashflow','dividend']
ix_list =  ['index_weight']
ix_symb_list = ['399300.SZ','h00906.CSI']#'000300.SH']

def fake_data(df):
    df["open"] = df["accum_nav"]
    df["high"] = df["adj_nav"]
    df["low"]  = df["adj_nav"]
    df["close"] = df["adj_nav"]
    df["volume"] = np.sign(df["adj_nav"])*1e9
    df["adjusted"] = df["adj_nav"]
    return df

def get_db_data(d_path,sd,ed,uname,bdt_list=None,dk = 'opt',d_type='daily',oflag=False,lflag=False):
    b_path = d_path + 'backup/'
    fuidx_flds = ['date','open','high','low','close','volume','settle','oi'] 
    basic_flds = ['date', 'open', 'high', 'low', 'close', 'volume','adjusted']   
    flds = fuidx_flds if dk in ('fut_index','fut','opt') else basic_flds
    zdict =fut_dict if dk in ('fut_index','fut') else eval(dk+'_dict') 
    for k,ex in zdict.items():
        root_dir = '/work/'+uname+'/db/'+d_type+'/'#'/work/jzhu/db/daily/' if d_type in fs_list else 
        os.makedirs(root_dir,exist_ok=True)

        shortname = 'fut' + '_' + ex + '.db' if dk in ('fut_index',) else dk + '_' + ex + '.db' #h( dk + '_'+d_type+'_' + ex + '.db' if d_type in fs_list  else  dk + '_' + ex + '.db')


        dirstr = root_dir + shortname#'fut' + '_' + ex + '.db' if dk in ('fut_index',) else root_dir + dk + '_' + ex + '.db'
        print(ex,dirstr)
        ded = create_engine('sqlite:///' + dirstr)
        symbols = ded.table_names()#db.collection_names()
        hist_path = d_path + 'sql/' + ex + '/' + d_type + '/'
        back_path  = b_path + ed
        dst_path = d_path + d_type + '/'
        os.makedirs(hist_path,exist_ok=True)
        os.makedirs(back_path,exist_ok=True)
        os.makedirs(dst_path,exist_ok=True)
        print(hist_path,back_path,dst_path)
        if os.path.exists(hist_path) and (not os.path.exists(back_path)):
            shutil.copytree(hist_path, back_path)
        for i in symbols:
            if i == 'not 000068.SZ':
                print('skipping0:',i)
                continue

            if (dk == 'fut' and filter_fut_symb(i)) or (dk == 'index' and (not i in ix_symb_list)):
                print('skipping ', i)
                continue

            stmp = i.lower() if dk in ('fut_index') else i
            fout = hist_path + stmp + '.csv' 
            print(fout)
            if lflag:
                if not filter_opt_symb(i):
                    fdst = dst_path + i + '.csv'
                    print(fdst)
                    if not os.path.realpath(fdst) == fout:
                        os.symlink(fout,fdst)
                    else:
                        print('symlink file exists, then skipped')
            else: 
                #df = pd.DataFrame([doc for doc in db[s].find()])
                df = pd.read_sql_table(table_name=i, con=ded)
                df = fake_data(df) if dk == 'fund_nav' and d_type == 'daily' else df
                print(df)
                cdf = df if d_type == 'basic' or d_type in fs_list or d_type in ix_list  else df[flds]
                sortkey = 'ts_code' if d_type == 'basic' else 'ann_date' if (d_type in fs_list) and \
                    (not re.match(r'^daily.*',d_type))   else 'date'
                cdf =cdf.sort_values(by=sortkey) if d_type in ix_list else cdf.sort_values(by=sortkey).drop_duplicates(subset=[sortkey],keep='last')
                print('before',cdf[sortkey] )#hack
                if d_type == 'daily' or d_type in ix_list:
                    cdf = cdf[cdf[sortkey] <= pd.to_datetime(ed).strftime("%Y%m%d")]#hack
                print(ed)
                print('after',cdf[sortkey] )#hack
                cdf.set_index([sortkey],inplace=True)

                print('jzxy',(cdf.index))
                dtfmt = "%Y%m%d" #hack 20191017 if  dk in ('fut_index',) else  "%Y-%m-%d"
                if d_type == 'daily' or d_type in ix_list:
                    cdf.index = pd.to_datetime(cdf.index).strftime(dtfmt) 
                    cdf = cdf[cdf.index.isin(bdt_list)]
                cdf.index.name = sortkey
                cdict = {}#hack 20191017 {'volume':"vol"} if dk in ('fut_index',) and d_type == 'daily'  else {}
                cdf = cdf.rename(columns = cdict)


                if oflag:
                    cdf.sort_index().round(7).to_csv(fout, index=True,na_rep='')
                    print(cdf.iloc[-5:,])
        ded.dispose()
    
def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"d:u:hoclv",["datakey=", "help"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)
    uname = pwd.getpwuid(os.getuid()).pw_name
    sys.path.append('/work/'+uname+'/project/zlib/')
    from zutils import get_prev_business_date, get_business_date_list
    bdl = get_business_date_list(fmt='%Y%m%d')
    output_flag = False
    conv_flag = False 
    link_flag = False 
    verbose = False
    dkey = 'opt'
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-d","--datakey"):
            dkey = a
        elif o == '-u':
            uname = a
        elif o == '-o':
            output_flag = True
        elif o == '-c':
            conv_flag = True
        elif o == '-l':
            link_flag = True
        else:
            assert False, 'unhandled option'




    print(dkey)
    edate = get_prev_business_date(date.today(), -1)#.strftime("%Y%m%d")
    sdate = get_prev_business_date(date.today() - timedelta(7), -1)#.strftime("%Y%m%d")
    print(sdate,edate)

    input_path = '/work/'+uname+'/input/' + dkey + '/'


    if dkey in ('opt','fut','fut_index','fund_nav','index','stock'):
        if dkey in ('stock'):
            for k in fs_list:
                get_db_data(input_path,sdate,edate,uname,bdt_list=bdl,dk=dkey, d_type=k,oflag=output_flag,lflag=link_flag)
        elif dkey in ('index'):
            for k in ix_list:
                get_db_data(input_path,sdate,edate,uname,bdt_list=bdl,dk=dkey, d_type=k,oflag=output_flag,lflag=link_flag)
            
        get_db_data(input_path,sdate,edate,uname,bdt_list=bdl,dk=dkey, d_type='basic',oflag=output_flag,lflag=link_flag)
        get_db_data(input_path,sdate,edate,uname,bdt_list=bdl,dk=dkey, d_type='daily',oflag=output_flag,lflag=link_flag)
            
if __name__ == '__main__':
    main()
    
