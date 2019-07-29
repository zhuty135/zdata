#!/usr/bin/python3
# extract data from tushare
import sys
import os
import pwd
uname = pwd.getpwuid(os.getuid()).pw_name
sys.path.append('../zlib/')

from zutils import get_prev_business_date 

import tushare as ts
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


import pandas as pd 
import numpy as np 
import os
import re
import shutil
import time
from sqlalchemy import create_engine
from pprint import pprint


from datetime import date, datetime, timedelta
from zipline.utils.calendars import get_calendar


fut_dict = {'CFFEX':'cfx','DCE':'dce','CZCE':'zce','SHFE':'shf','INE':'ine'}
opt_dict = {'DCE':'dce',
            'CZCE':'zce',
            'SHFE':'shf', 
            'SSE':'sse'
             }
fund_dict = {'E':'e','O':'o'}
index_dict = {'CSI':'csi','SSE':'sse','SZSE':'szse'}
stock_dict = {'SSE':'sse','SZSE':'szse'}
futs_all_static_fields='ts_code,symbol,exchange,name,fut_code,multiplier,trade_unit,per_unit,quote_unit,quote_unit_desc,d_mode_desc,list_date,delist_date,d_month,last_ddate,trade_time_desc'

def check_and_delete_db_record(s,mdb_str,del_flag=False):
    db=sqlite3.connect('/tmp/checkdelete.db')
    if del_flag:
        try:
            db[s].remove({"date": {"$gt": "2019-05-15"}})
        except Exception as e:
            print(e) 
    if mdb_str.split('_')[0] == 'fund':
        print(mdb_str)
        print([doc['end_date']  for doc in db[s].find().sort('end_date')])
    else:
        print([doc['date']  for doc in db[s].find().sort('date')])


index_fld = "['ts_code', 'name', 'fullname', 'market', 'publisher', 'index_type', 'category', 'base_date', 'base_point', 'list_date', 'weight_rule', 'desc',  'exp_date']"
stock_fld = "['ts_code', 'symbol', 'name', 'area', 'industry', 'fullname', 'enname', 'market', 'exchange', 'curr_type', 'list_status', 'list_date', 'delist_date', 'is_hs']"
def get_tu_basic(k,dk,d_type,verbose=False):
    p = 'pro.fund' if dk in ('fund','fund_nav') else 'pro.'+ dk
    m = 'market' if dk in ('fund','fund_nav','index',) else 'exchange'
    f = index_fld  if dk in ('fund','fund_nav','index',) else stock_fld 
    funcstr= p + '_' + d_type + "(" + m + "='" + k + "',fields=" + f + ")" if dk in ('index','stock') else p + '_' + d_type + "(" + m + "='" + k + "')" 
    print('funcstr', funcstr)
    df=eval(funcstr)
    return df


def basic_to_db(dk,ex,d_type, df, oflag, verbose=True):
    if verbose:
        epath = '/work/'+ uname + '/db/basic/'  
        os.makedirs(epath,exist_ok=True)
        dbname = dk + '_' + ex + '.db'
        estr = epath + dbname
        deb = create_engine('sqlite:///' + estr)
        print(estr)
        check = deb.has_table(d_type)
        if check:
            doc = pd.read_sql_table(table_name=d_type, con=deb)
            print('Existing table', d_type, doc['ts_code'][-5:-1])
            doclist = doc['ts_code']
            if verbose:  df.to_csv('/tmp/udf.'+dk+'.'+ex)
            udf = df[~df['ts_code'].isin(doclist)]
        else:
            udf = df
        if udf.empty:
            print("NO update for ", estr,d_type)
            return False
        elif oflag:
            try:
                print("==========Updating for ", estr,d_type)
                udf.to_sql(name=d_type, con=deb, if_exists='append')
            except Exception as e:
                print(e)
        deb.dispose()    
        return True 


def fake_data(df):
    df["open"] = df["adj_nav"]
    df["high"] = df["adj_nav"]
    df["low"]  = df["adj_nav"]
    df["close"] = df["adj_nav"]
    df["volume"] = np.sign(df["adj_nav"])*1e9
    df["adjusted"] = df["adj_nav"]
    return df

def toggle_start_date(i,s,symbols,mcod):
    if i in symbols:
        if not mcod.find().count() == 0:
            dtlist = [doc['date'] for doc in mcod.find().sort('date')]
            print('old s',s,dtlist[-1])
            s = (pd.to_datetime(dtlist[-1])+timedelta(1)).strftime("%Y%m%d")  if s > dtlist[-1] else s
    return s

def remove_duplicates(coll):
    p = [
        {"$group": {"_id": "$date", "unique_ids": {"$addToSet": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": { "$gte": 2 }}}
    ]
    cursor = coll.aggregate(p)
    response = []
    print('cursor',cursor)
    for doc in cursor:
        del doc["unique_ids"][0]
        for id in doc["unique_ids"]:
            response.append(id)

    result = coll.remove({"_id": {"$in": response}})
    return result

fs_list =  ['dividend']
def write_to_db(i,df, ded, fflag, oflag, cdict,keystr='date',verbose=True):
    if df is None or df.empty:
        print('skipping7',i)
        return False
    df = df.rename(columns = cdict)
    print('latest data:')
    print(df.iloc[-5:,])
    if ded.has_table(i):
        existing_dates_list = pd.read_sql_table(table_name=i, con=ded)[keystr]
        print('existing_dates_df',existing_dates_list.sort_values()[-5:])
        df = df[~df[keystr].isin(existing_dates_list)]
        if df.empty:    
            return False
    print('after filtering',df.iloc[-5:,])
    if oflag:
        try:
            if fflag:
                print('FULL HISTORY drop then insert')
                df.to_sql(name=i, con=ded, if_exists='replace',index=False)
            else:
                df.to_sql(name=i, con=ded, if_exists='append',index=False)
        except Exception as e:
            print(e)
        ddf = pd.read_sql_table(table_name=i, con=ded).set_index([keystr])
        print('ddf begins',ddf.sort_index().iloc[1:5,])
        print('ddf ends',ddf.sort_index().iloc[-5:,])
    return True

def fetch_fs_data(i,f,s,e,dk):
    fcallbasic = 'pro.' + f + "(ts_code='"+ i + "',start_date='" + s + "',end_date='" + e 
    fcall = fcallbasic  + "')"
    print(fcall)
    df = eval(fcall) 
    if df is None or df.empty:
        return None 
    return df

adict = {'stock':'E', 'index':'I','fut':'FT','fund':'FD','fund_nav':'FD','opt':'O', }
def fetch_daily_data(i,s,e,dk):
    a =  "ts.pro_bar(ts_code='"+ i + "',asset='" + adict[dk]  
    fcallbasic =  a + "',start_date='" + s + "',end_date='" + e 
    fcall =  fcallbasic + "',adj='hfq')" if dk =='stock' else  fcallbasic + "')"
    print(fcall)
    df = eval(fcall) 
    if df is None or df.empty:
        return None 
    time.sleep(1)
    if dk in ('fut','opt'): 
        df["adjusted"] = df['settle']
    else:
        df["adjusted"] = df["close"]
    return df

def bar_to_db(dk,ex,d_type,sd,ed,fflag,oflag,verbose=True):
    estr = '/work/'+uname+'/db/basic/'  + dk + '_' + ex + '.db'
    deb = create_engine('sqlite:///' + estr)
    shortname =  dk + '_' + ex + '.db'
    dailypath = '/work/' + uname + '/db/' 
    os.makedirs(dailypath,exist_ok=True)
    dailystr = dailypath + shortname
    ded = create_engine('sqlite:///' + dailystr)

    print(estr,dailystr)
    symbols = ded.table_names()
    print(symbols)
        
    bdf = pd.read_sql_table(table_name='basic', con=deb)
    bdf = bdf.sort_values(by='ts_code').drop_duplicates(subset=['ts_code'],keep='last')
    bdf.set_index(['ts_code'],inplace=True)

    if verbose:
        if True:
            for i in  bdf.index:
                cdict = {}
                dedt =  bdf.loc[bdf.index==i,'exp_date'][0] if dk in ('index',) else bdf.loc[bdf.index==i,'delist_date'][0]
                dedt = pd.to_datetime(dedt).strftime("%Y%m%d") if dedt is not None else dedt
                sdt_str = 'found_date' if dk in ('fund_nav','fund') else 'list_date'
                s = sd if (re.match(r'^daily.*',d_type) and not fflag) else bdf.loc[bdf.index==i,sdt_str][0] 
                s = pd.to_datetime(s).strftime("%Y%m%d") if s is not None else s
                e = pd.to_datetime(ed).strftime('%Y%m%d') if (re.match(r'^daily.*', d_type) and not fflag) or isinstance(dedt, type(None)) else dedt 
                bdf.to_csv('/tmp/bdf.csv')

                print('checking',i,dailystr)
                print('bdf',bdf.loc[bdf.index==i,])
                if dk in ('fut','opt') and (isinstance(dedt, type(None)) or dedt < e ) : 
                    print('skipping1',i,dedt,e)
                    continue
                elif dk in ('fund_nav','fund','index'):
                    if not isinstance(dedt, type(None)) and dedt < e  : 
                        print('skipping2',i)
                        continue

                print('starting',i,'start',s,'end',e) 
                if s is None or s > e:
                    print('skipping3',i)
                    continue
                if dk in ('fund_nav','fund'): 
                    if d_type=='daily':
                        fcallbasic = "pro." + dk + "(end_date='" + e 
                    else:
                        fcallbasic = 'pro.' + dk + "(ts_code='"+ i 
                        time.sleep(1)
                    fcall = 'pro.' + dk + "(ts_code='"+ i + "')"  
                    df = eval(fcall).sort_values(by=['end_date']) 
                    if df.empty:
                        print('skipping4',i)
                        continue
                    print('df first line',df.iloc[0,])
                    print('df last line',df.iloc[-1,])
                    df = df[df['end_date'] >= s]

                    print('jxxx',df.iloc[-3:,])
                    if isinstance(df['adj_nav'],type(None)):
                        print('skipping5',i)
                        continue
                    df = fake_data(df)
                    cdict = {'end_date':'date'}
                    ks = 'date'
                    wf = write_to_db(i,df, ded, fflag, oflag,cdict,keystr=ks)
                elif dk == 'stock' and d_type in fs_list: 
                    df = fetch_fs_data(i,d_type,s,e,dk)
                    print(df)
                    ks = 'end_date'
                    wf = write_to_db(i,df, ded, fflag, oflag,cdict,keystr=ks)
                else:
                    df = fetch_daily_data(i,s,e,dk)
                    cdict = {'trade_date':'date','vol':"volume"}
                    ks = 'date'
                    wf = write_to_db(i,df, ded, fflag,oflag,cdict,keystr=ks)
                if wf:
                    print('writing ',i,' to', dailystr)
        ded.dispose()    
        deb.dispose()    
    
def get_tu_data(d_path,sd,ed,dk = 'opt',d_type='basic',fflag=False,oflag=False,verbose=True):
    zdict = opt_dict if dk == 'opt' else (fut_dict if dk == 'fut' else (index_dict if dk == 'index' else (stock_dict if dk == 'stock' else fund_dict)))
    for k,ex in zdict.items():
        if d_type == 'basic' :
            df = get_tu_basic(k,dk,d_type,verbose=False)
            if verbose: print(df)
            result = basic_to_db(dk,ex,d_type, df, oflag, verbose=True)
        else:
            result = bar_to_db(dk,ex,d_type,sd,ed, fflag, oflag, verbose=True)

def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"d:t:r:n:hfocv",["datakey=", "help"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)
    output_flag = False
    fullhist_flag = False 
    conv_flag = False 
    verbose = False
    dkey = 'fund_nav'
    test_code = None
    remove_flag = False 
    n = -1 
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-d","--datakey"):
            dkey = a
        elif o == '-n':
            n = eval(a)
        elif o == '-t':
            test_code = a 
        elif o in ('-r', '--remove'):
            remove_flag = True 
        elif o == '-f':
            fullhist_flag = True
        elif o == '-o':
            output_flag = True
        elif o == '-c':
            conv_flag = True
        else:
            assert False, 'unhandled option'
    print(dkey)

    if test_code is not None:
        mfs = test_code.split('.')
        mdb_str = dkey + '_' + mfs[1].lower() + '_daily'
        print(mdb_str)
        check_and_delete_db_record(test_code,mdb_str,del_flag=remove_flag)
        exit(0)

    edate = get_prev_business_date(date.today(), -1)
    sdate = get_prev_business_date(date.today() - timedelta(7), -1)
    print(sdate,edate)

    input_path = '/work/'+uname+'/db/' + dkey + '/'

    if dkey in ('opt','fut','fund_nav','index','stock'):
        if fullhist_flag:
            if dkey in ('stock',):
                for k in fs_list:
                    print('k',k)
                    get_tu_data(input_path,sdate,edate,dk=dkey, d_type=k,fflag=fullhist_flag,oflag=output_flag)
            else:
                get_tu_data(input_path,sdate,edate,dk=dkey, d_type='daily',fflag=fullhist_flag,oflag=output_flag)
        else:
            get_tu_data(input_path,sdate,edate,dk=dkey, d_type='basic',oflag=output_flag)
            if dkey in ('stock',):
                for k in fs_list:
                    get_tu_data(input_path,sdate,edate,dk=dkey, d_type=k,oflag=output_flag)
                print(dkey)
            print('Look back date is ',n)
            sdate = get_prev_business_date(date.today(), n)
            get_tu_data(input_path,sdate,edate,dk=dkey, d_type='daily',oflag=output_flag)


if __name__ == '__main__':
    main()


