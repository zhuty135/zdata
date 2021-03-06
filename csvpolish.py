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

def fake_data(df,adjflag=False):
    if adjflag:
        df["adjusted"] = df.iloc[:,3]
    else:
        df["high"] = df.iloc[:,0]
        df["low"]  = df.iloc[:,0]
        df["close"] = df.iloc[:,0]
        df["volume"] = np.sign(df.iloc[:,0])*1e9
        df["adjusted"] = df.iloc[:,0]
    
    return df

def fill_missing_data(fin,fout,index_col,zfix):
    df = None
    try:
        df = pd.read_csv(fin,index_col=index_col,parse_dates=True)
    except Exception as err:
        print(str(err))
    dt_fmt='%Y-%m-%d'

    if df.shape[1] < 2:
        df.columns = ['open']
    
    if df.empty:
        return False
    sd = df.index[0].strftime(dt_fmt)
    ed = df.index[-1].strftime(dt_fmt)
    bd_list = get_business_date_list(fmt=dt_fmt)
    print(sd,ed,type(bd_list))
    short_bd_list = pd.to_datetime(bd_list[(bd_list >= sd) & (bd_list <= ed)])
    print('jzcheck', df.iloc[-10:,])
    print(short_bd_list)
    df.sort_index(inplace=True)
    try: 
        #df.index.df.drop_duplicates(inplace=True)
        df = df[~df.index.duplicated()]

        #df = df.drop_duplicates()
    except Exception as err:
        print('jzerror:', str(err))
    print('jzcheck2',df.iloc[-10:,])
    df.sort_index(inplace=True)
    df = df.reindex(short_bd_list,method='ffill')
    df = df.fillna(method='ffill') 

    if df.shape[1] < 2:
        df = fake_data(df)
    elif re.match(r'.*FX\.csv$',fin.split('/')[-1]):
        df = fake_data(df,adjflag=True)

    if zfix:
        zfix_dt = pd.to_datetime(bd_list[ bd_list > ed ][0])
        zseries  = df.iloc[-1,]
        zdf = pd.DataFrame(data=zseries,index=[zfix_dt])
        zseries.name = zfix_dt
        df = df.append(zseries)
        print('zfix: appended extra row',zfix_dt)
    df.index.names = ['date']
    df.sort_index().round(7).to_csv(fout, index=True,date_format=dt_fmt,na_rep='')
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

def start_polish(input_dir,output_dir,index_col,zfix):
    from os.path import isfile,join 
    files = [f for f in os.listdir(input_dir) if isfile(join(input_dir, f))]
    print(input_dir,output_dir)
    print(files)
    os.makedirs(output_dir,exist_ok=True)

    for f in files:
        fin = input_dir + f
        fout = output_dir + f
        if re.match(r'^.*\..*\.csv',f):
            fout = output_dir + f
        else:
            f_split = f.split('.')
            fout = output_dir + f_split[0]  + '.PO.' + f_split[-1] 
        print('fin',fin)
        fill_missing_data(fin,fout,index_col,zfix)
        print('fout',fout)


def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"i:k:o:zv",["index_col=","help"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)
    verbose = False
    root_dir = '/work/'+uname+'/'#os.getcwd()
    input_dir  = None
    output_dir = None 
    index_col =  'date' 
    zfix = False
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-i"):
            input_dir  = a
        elif o == ('-o'):
            output_dir = a 
        elif o == ('-z'):
            zfix = True 
        elif o == ('--index_col'):
            index_col =  a 
    if input_dir is not None:
        if input_dir.find('/')!= -1 :
            id_split = input_dir.split('/')
            print('id_split', id_split[-1] )
            key_str = '/'.join(id_split[-5:-1]) if id_split[-1] == '' else '/'.join(id_split[-2:])
            output_dir = root_dir + '/data/pol/'  + key_str + '/' 
        else:
            output_dir = root_dir + '/data/pol/'  + input_dir + '/' 
            input_dir  = root_dir + '/input/' + input_dir + '/' 
    else:
        print('input_dir is missing')

    print(input_dir)
    input_dir = input_dir + '/' 
    print('output_dir',output_dir) 
    
    start_polish(input_dir,output_dir,index_col,zfix)
    assert(0)

    d_type='stock'
    i_type='cnix_399300_sz'
    ix_weight_file = '/work/jzhu/input/index/sql/szse/index_weight/399300.sz.csv'
    create_symb_link(d_type,i_type,ix_weight_file)
    tmp_list = i.split('.')

if __name__ == '__main__':
    main()

