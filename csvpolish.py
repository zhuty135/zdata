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

def fill_missing_data(fin,fout,index_fld):
    df = None
    try:
        df = pd.read_csv(fin,index_col=index_fld)
    except Exception as err:
        print(str(err))
    df.columns = ['close']
    sd = df.index[0]
    ed = df.index[-1]
    bd_list = get_business_date_list()
    bd_list = bd_list[(bd_list >= sd) & (bd_list <= ed)]
    df = df.reindex(bd_list,method='ffill')
    df = fake_data(df)

    print(sd,ed,bd_list)
    col_value=['open','high','low','close']
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
        fill_missing_data(forig,ffake,index_fld)
        print(forig)
        print(ffake)
        if filter_link_symb(i):
            fdst = dst_path + i + '.csv'
            print(fdst)
            if not os.path.realpath(fdst) == ffake:
                os.symlink(ffake,fdst)
            else:
                print('symlink file exists, then skipped')

def start_polish(input_dir,output_dir,index_fld):
    from os.path import isfile,join 
    files = [f for f in os.listdir(input_dir) if isfile(join(input_dir, f))]
    print(files)
    os.makedirs(output_dir,exist_ok=True)

    for f in files:
        fin = input_dir + f
        fout = output_dir + f
        fill_missing_data(fin,fout,index_fld)


def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"i:k:o:v",["index_fld=","help"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)
    verbose = False
    root_dir = '/work/'+uname+'/'#os.getcwd()
    key_str = 'Index'
    input_dir  = root_dir + '/input/' + key_str + '/'
    output_dir = root_dir + '/data/pol/'  + key_str + '/'
    index_fld =  'date' 
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-i"):
            input_dir  = a
        elif o in ("-k"):
            key_str= a
        elif o == ('-o'):
            output_dir = a 
        elif o == ('--index_fld'):
            index_fld =  a 
    output_dir = output_dir if re.match(r'.*\/.*', output_dir)  else output_dir + key_str + '/'
    start_polish(input_dir,output_dir,index_fld)
    assert(0)

    d_type='stock'
    i_type='cnix_399300_sz'
    ix_weight_file = '/work/jzhu/input/index/sql/szse/index_weight/399300.sz.csv'
    create_symb_link(d_type,i_type,ix_weight_file)
    tmp_list = i.split('.')

if __name__ == '__main__':
    main()

