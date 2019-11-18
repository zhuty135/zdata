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

def get_univ(iwf):
    df = pd.read_csv(iwf)

    bdf = df.sort_values(by='con_code').drop_duplicates(subset=['con_code'],keep='last')
    udf = bdf['con_code'].filter(regex=r'^\d{6}.*')

    print('coutn',udf.count())
    return udf.sort_values().tolist()

def fake_data(df,origin_fld='close'):
    df["open"] = df[origin_fld]
    df["high"] = df[origin_fld]
    df["low"]  = df[origin_fld]
    df["close"] = df[origin_fld]
    df["volume"] = np.sign(df[origin_fld])*1e9
    df["adjusted"] = df[origin_fld]
    return df

def fill_missing_data(fin,fout,index_col):
    df = None
    try:
        df = pd.read_csv(fin,index_col=index_col,parse_dates=True)
    except Exception as err:
        print(str(err))
    dt_fmt='%Y%m%d'
    df.index.names = ['date']

    if df.shape[1] < 2:
        df.columns = ['close']
    
    if df.empty:
        return False
    sd = df.index[0].strftime(dt_fmt)
    ed = df.index[-1].strftime(dt_fmt)
    bd_list = get_business_date_list(fmt=dt_fmt)
    print(sd,ed,type(bd_list))
    bd_list = pd.to_datetime(bd_list[(bd_list >= sd) & (bd_list <= ed)])
    df = df.reindex(bd_list,method='ffill') 

    if df.shape[1] < 2:
        df = fake_data(df)

    df.sort_index().round(7).to_csv(fout, index=True,na_rep='')
    return True

def filter_link_symb(i):
    return True

ex_dict = {'sh':'sse','sz':'szse'}
def get_ex(i):
    tmp_list = i.split('.')
    return ex_dict[tmp_list[1].lower()]

def create_symb_link(d_type,i_type,ix_weight_file):
    ulist = get_univ(ix_weight_file)

    dst_path = '/work/'+uname+'/input/' + i_type + '/daily/' 
    fake_path = '/work/'+uname+'/input/' + i_type + '/hack/' 
    os.makedirs(fake_path,exist_ok=True)

    os.makedirs(dst_path, exist_ok=True)

    for i in ulist:
        ex = get_ex(i)
        hist_path = '/work/'+uname+'/input/' + d_type + '/sql/' + ex + '/daily/'
        forig = hist_path + i + '.csv'
        ffake = fake_path + i + '.csv'
        print('forig',forig)
        print('ffake',ffake)
        fill_missing_data(forig,ffake,index_col)
        if filter_link_symb(i):
            fdst = dst_path + i + '.csv'
            print(fdst)
            if not os.path.realpath(fdst) == ffake:
                os.symlink(ffake,fdst)
            else:
                print('symlink file exists, then skipped')

def start_polish(input_dir,output_dir,index_col):
    from os.path import isfile,join 
    files = [f for f in os.listdir(input_dir) if isfile(join(input_dir, f))]
    print(input_dir,output_dir)
    print(files)
    os.makedirs(output_dir,exist_ok=True)

    for f in files:
        fin = input_dir + f
        fout = output_dir + f
        print('fin',fin)
        fill_missing_data(fin,fout,index_col)
        print('fout',fout)


def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"i:k:o:v",["index_col=","help"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)
    verbose = False
    root_dir = '/work/'+uname+'/'#os.getcwd()
    key_str = None 
    input_dir  = None
    output_dir = None 
    index_col =  'date' 
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-i"):
            input_dir  = a
        elif o in ("-k"):
            key_str= a
        elif o == ('-o'):
            output_dir = a 
        elif o == ('--index_col'):
            index_col =  a 
    if input_dir is not None:
        id_split = input_dir.split('/')
        print('id_split', id_split[-1] )
        
        key_str = id_split[-3]+'/'+id_split[-2] if id_split[-1] == '' else id_split[-2]+'/'+id_split[-1] 
        print(key_str)
    elif key_str is not None:
        input_dir  = root_dir + '/input/' + key_str + '/' 
    else:
        print('input_dir or key_str is missing')

    input_dir = input_dir + '/' 
    output_dir = root_dir + '/data/pol/'  + key_str + '/' 
    print('output_dir',output_dir) 
    
    start_polish(input_dir,output_dir,index_col)
    assert(0)

    d_type='stock'
    i_type='cnix_399300_sz'
    ix_weight_file = '/work/jzhu/input/index/sql/szse/index_weight/399300.sz.csv'
    create_symb_link(d_type,i_type,ix_weight_file)
    tmp_list = i.split('.')

if __name__ == '__main__':
    main()

