#!/usr/bin/python3
# extract data from tushare
import sys
import os
import pwd
uname = pwd.getpwuid(os.getuid()).pw_name
sys.path.append('/work/'+uname+ '/project/zlib/')

from zutils import get_prev_business_date,get_business_date_list

import tushare as ts
def get_token():
    import configparser
    cp = configparser.ConfigParser()
    cp.read('/work/'+uname+'/project/factors/config/databasic.cfg')
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

def basic_to_db(dk,ex,d_type, df, aflag, oflag, verbose=True):
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
            print('Existing table', d_type, doc['ts_code'][-2:-1])
            doclist = doc['ts_code']
            #if verbose: df.to_csv('/tmp/udf.'+dk+'.'+ex)
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
            except Exception as err:
                print(err)
        deb.dispose()    
        return True 

fs_list =  ['daily_basic','dividend','fina_indicator','income','balancesheet','cashflow']
ix_list =  ['index_weight']#, 'index_dailybasic','index_member']
ix_symb_list = ['399300.SZ','h00906.CSI']#'000300.SH']

def write_to_db(i,df, ded, aflag, fflag, oflag, cdict,keystr='date',verbose=False):
    if df is None or df.empty:
        print('skipping7',i)
        return False
    df = df.rename(columns = cdict)
    if ded.has_table(i):
        existing_dates_list = pd.read_sql_table(table_name=i, con=ded)[keystr]
        print('existing_dates_df',existing_dates_list.sort_values()[0:-1])
        df = df[~df[keystr].isin(existing_dates_list)]
        if df.empty:    
            return False
    print('after filtering',df.iloc[0:0,],df.iloc[-1:,])
    if oflag:
        try:
            if fflag and (not aflag):
                print('FULL HISTORY drop then insert')
                df.to_sql(name=i, con=ded, if_exists='replace',index=False)
            else:
                print('PART HISTORY only append')
                df.to_sql(name=i, con=ded, if_exists='append',index=False)
        except Exception as e:
            print(e)
        ddf = pd.read_sql_table(table_name=i, con=ded).set_index([keystr])
        if verbose: print('ddf begins',ddf.sort_index().iloc[1:3,])
        if verbose: print('ddf ends',ddf.sort_index().iloc[-3:,])
    return True

    
def fetch_fs_data(i,f,s,e,dk):
    fcallbasic = 'pro.' + f + "(ts_code='"+ i + "',start_date='" + s + "',end_date='" + e 
    fcall = fcallbasic  + "')"
    print(fcall)
    df = eval(fcall) 
    if df is None or df.empty:
        return None 
    return df

def fetch_fund_data(i,s,e,dk):
    fcall = 'pro.' + dk + "(ts_code='"+ i + "')"  
    df = None
    try:
        df = eval(fcall).sort_values(by=['end_date']) 
    except Exception as e:
        print(e)
    if df is None or df.empty:
        print('skipping4',i)
        return None#continue
    df = df[df['end_date'] >= s]
    print('jxxx',df.iloc[-3:,])
    if isinstance(df['adj_nav'],type(None)):
        print('skipping5',i)
        return None#continue
    return df

def fetch_index_data(i,f,s,e,dk):
    date_sun = pd.date_range(start = s, end =e, freq = 'W-SUN').strftime("%Y%m%d")
    date_sun = np.append(date_sun,e)
    df = pd.DataFrame()
    stmp = s 
    for x in date_sun:
        etmp = x
        fcallbasic = 'pro.' + f + "(index_code='"+ i + "',start_date='" + s + "',end_date='" + e 
        fcall = fcallbasic  + "')"
        tmpdf = eval(fcall) 
        stmp = etmp 
        if tmpdf is None or tmpdf.empty :
            print('Returning None')
            continue
        df = pd.concat([df, tmpdf]).drop_duplicates()
        time.sleep(0.12)
    return df

adict = {'stock':'E', 'index':'I','fut':'FT','fund':'FD','fund_nav':'FD','opt':'O', }
def fetch_daily_data(i,s,e,dk):
    a =  "ts.pro_bar(ts_code='"+ i + "',asset='" + adict[dk]  
    fcallbasic =  a + "',start_date='" + s + "',end_date='" + e + "')"
    fcall =  fcallbasic #+ "',adj='hfq')" if dk =='stock' else  fcallbasic + "')"
    df = eval(fcall) 
    if df is None or df.empty:
        return None 
    time.sleep(0.12)
    if dk in ('fut','opt'): 
        df["adjusted"] = df['settle']
    else:
        df["adjusted"] = df["close"]
    return df

def amend_daily_data(i,sd,ed,dk,ded):
    dt_series = None
    try:
        dt_series = (pd.read_sql_table(table_name=i, con=ded)['date'].sort_values())
    except Exception as e:
        print(e)
    df = None
    if dt_series is None or dt_series.empty:
        if dk == 'fund_nav':
            df = fetch_fund_data(i,sd,ed,dk) 
        #elif dk == 'index':
        #    df = fetch_index_data(i,sd,ed,dk)
        else:
            df = fetch_daily_data(i,sd,ed,dk) 
    else:
        dt_set = set(dt_series)
        bd_list = get_business_date_list(fmt='%Y%m%d')
        print('bd_list0',bd_list)
        print('sd/ed',sd,ed)
        bd_list = (bd_list[(bd_list > sd) & (bd_list < ed)])
        bd_set = set(bd_list)
        missing_dates = dt_set.union(bd_set)  - dt_set.intersection(bd_set)
        missing_dates = sorted(list(missing_dates))
        df = pd.DataFrame()
        if missing_dates is None:
            return None 
        print('missing_dates',(missing_dates))
        print('dt_series',dt_series)
        print('bd_list',bd_list)
        
        dt_begin = missing_dates[0]
        for dt in missing_dates[1:] :
            pd_dt = pd.to_datetime(dt)
            dt_diff = pd_dt - pd.to_datetime(dt_begin)
            if dt_diff < timedelta(7):
                continue
            dt_end = (pd_dt - timedelta(1)).strftime('%Y%m%d')
            if dt_diff > timedelta(31):
                dt_end = dt_begin
            tmpdf = fetch_daily_data(i, dt_begin, dt_end, dk)
            print('amending date:',dt_begin,dt_end,dt_diff)
            dt_begin = dt 
            time.sleep(0.10)
            if tmpdf is None:
                continue
            print(tmpdf)
            df = pd.concat([df, tmpdf]).drop_duplicates()
    print('amend_daily_data',i)
    print(df)
    return df

def bar_to_db(dk,ex,d_type,sd,ed,aflag,dlflag,fflag,oflag,verbose=True):
    estr = '/work/'+uname+'/db/basic/'  + dk + '_' + ex + '.db'
    deb = create_engine('sqlite:///' + estr)
    shortname =  dk + '_' + ex + '.db'
    dailypath = '/work/' + uname + '/db/' + d_type + '/'
    os.makedirs(dailypath,exist_ok=True)
    dailystr = dailypath + shortname
    ded = create_engine('sqlite:///' + dailystr)

    print(estr,dailystr)
    symbols = ded.table_names()
    print(symbols)
       
    if dlflag: 
        bdf = pd.DataFrame({'ts_code':['000024.SZ',]})
    else:
        bdf = pd.read_sql_table(table_name='basic', con=deb)
        bdf = bdf.sort_values(by='ts_code').drop_duplicates(subset=['ts_code'],keep='last')
    bdf.set_index(['ts_code'],inplace=True)

    if verbose:
        if True:
            for i in  bdf.index:
                print ('testjz',i)
                if i == 'not 000002.SZ':
                    print('skipping0:',i)
                    continue 
                if dk == 'index' and d_type in ix_list and (not i in ix_symb_list):   
                    print('skipping index',i)
                    continue

                time.sleep(0.12)
                cdict = {}
                if dlflag:
                    dedt = ed 
                else:
                    dedt =  bdf.loc[bdf.index==i,'exp_date'][0] if dk in ('index',) else bdf.loc[bdf.index==i,'delist_date'][0]
                dedt = pd.to_datetime(dedt).strftime("%Y%m%d") if dedt is not None else dedt
                sdt_str = 'found_date' if dk in ('fund_nav','fund') else 'list_date'
                s = sd if (re.match(r'^[daily|index_weight].*',d_type) and not fflag) or dlflag else bdf.loc[bdf.index==i,sdt_str][0] 
                s = pd.to_datetime(s).strftime("%Y%m%d") if s is not None else s
                e = pd.to_datetime(ed).strftime('%Y%m%d') if (re.match(r'^daily.*', d_type) and not fflag) or isinstance(dedt, type(None)) else dedt 

                print('checking',i,dailystr)
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
                ks = 'date'
                if dk in ('fund_nav','fund') and not aflag: 
                    cdict = {'end_date':ks}
                    df = fetch_fund_data(i,s,e,dk) 
                elif dk == 'stock' and d_type in fs_list: 
                    cdict = {'trade_date':'date'}  if d_type == 'daily_basic' else cdict
                    ks = ks if d_type == 'daily_basic' else 'end_date'
                    df = fetch_fs_data(i,d_type,s,e,dk) 
                elif dk == 'index' and d_type in ix_list: 
                    cdict = {'trade_date':'date'} 
                    df = fetch_index_data(i,d_type,s,e,dk) 
                else:
                    df = amend_daily_data(i,s,e,dk,ded) if aflag else fetch_daily_data(i,s,e,dk)
                    cdict = {'trade_date':'date','vol':"volume"}
                if df is None or df.empty:
                    continue
                wf = write_to_db(i,df, ded, aflag, fflag, oflag,cdict,keystr=ks)
                if wf:
                    print('writing ',i,' to', dailystr)
        ded.dispose()    
        deb.dispose()    
    
def get_tu_data(d_path,sd,ed,dk = 'opt',d_type='basic',aflag=False,dlflag=False,fflag=False,oflag=False,verbose=True):
    zdict = opt_dict if dk == 'opt' else (fut_dict if dk == 'fut' else (index_dict if dk == 'index' else (stock_dict if dk == 'stock' else fund_dict)))
    for k,ex in zdict.items():
        if d_type == 'basic' :
            df = get_tu_basic(k,dk,d_type,verbose=False)
            if verbose: print(df.iloc[-1,])
            result = basic_to_db(dk,ex,d_type, df, aflag, oflag, verbose=True)
        else:
            result = bar_to_db(dk,ex,d_type,sd,ed, aflag, dlflag, fflag, oflag, verbose=True)

def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"d:t:rn:s:e:ahfocv",["datakey=", "help"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)
    output_flag = False
    fullhist_flag = False 
    amend_flag = False 
    conv_flag = False 
    verbose = False
    dkey = 'fund_nav'
    test_code = None
    sdate = None
    edate = None
    delist_flag = False 
    n =  -7 
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-d","--datakey"):
            dkey = a
        elif o == '-n':
            n = eval(a)
        elif o == '-t':
            test_code = a 
        elif o in ('-r'):
            delist_flag = True 
        elif o == '-f':
            fullhist_flag = True
        elif o == '-a':
            amend_flag = True
        elif o == '-o':
            output_flag = True
        elif o == '-c':
            conv_flag = True
        elif o == '-s':
            sdate = a 
        elif o == '-e':
            edate = a 
        else:
            assert False, 'unhandled option'
    print(dkey)

    edate = get_prev_business_date(date.today(), -1) if edate is None else edate
    sdate = get_prev_business_date(date.today(),  n) if sdate is None else sdate
    print(sdate,edate)

    input_path = '/work/'+uname+'/db/' + dkey + '/'

    if dkey in ('opt','fut','fund_nav','index','stock'):
        get_tu_data(input_path,sdate,edate,dk=dkey, d_type='basic',aflag=amend_flag,oflag=output_flag)
        if fullhist_flag:
            get_tu_data(input_path,sdate,edate,dk=dkey, d_type='daily',aflag=amend_flag,dlflag=delist_flag, fflag=fullhist_flag,oflag=output_flag)
            if not amend_flag:
                if dkey in ('stock',) :
                    for k in fs_list:
                        print('k',k)
                        get_tu_data(input_path,sdate,edate,dk=dkey, d_type=k,dlflag=delist_flag,fflag=fullhist_flag,oflag=output_flag)
                elif dkey in ('index',) :
                    for k in ix_list:
                        print('ix_list k',k)
                        get_tu_data(input_path,sdate,edate,dk=dkey, d_type=k,fflag=fullhist_flag,oflag=output_flag)
        else:
            get_tu_data(input_path,sdate,edate,dk=dkey, d_type='daily',oflag=output_flag)
            assert(0)
            if dkey in ('stock',):
                for k in fs_list:
                    get_tu_data(input_path,sdate,edate,dk=dkey, d_type=k,oflag=output_flag)
            elif dkey in ('index',):
                for k in ix_list:
                    get_tu_data(input_path,sdate,edate,dk=dkey, d_type=k,oflag=output_flag)
            print(dkey)
            print('Look back date is ',n)


if __name__ == '__main__':
    main()


