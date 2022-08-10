#!/usr/local/anaconda3/bin/python3.9
import pandas as pd
import numpy as np
import sys
import os
import pwd
import re
uname = pwd.getpwuid(os.getuid()).pw_name
sys.path.append('/work/'+uname+ '/project/zlib/')

from zutils import get_prev_business_date, get_business_date_list


from os.path import isfile,join
def file_filter(f):
    if f[-4:] in ['.csv']:
        return True
    else:
        return False

def start_analysis(input_dir,output_dir,index_col,zfix):
    input_dir = '/work/jzhu/output/' + input_dir
    files = [f for f in os.listdir(input_dir) if isfile(join(input_dir, f))]
    csvfiles = list(filter(file_filter,files))
    print(input_dir,output_dir)
    print(csvfiles)
    os.makedirs(output_dir,exist_ok=True)

    tickerlist = [f.split('.')[0] for f in csvfiles] 

    totaldf = pd.DataFrame()
    
    for f in csvfiles:
        t = f.split('.csv')[0]
        fin = input_dir + f
        print(fin)
        fsize = os.path.getsize(fin)
        if not fsize == 0:
            df = pd.read_csv(fin,sep = '\s+',names=['sr','ret','vol','dd','txns'])
            totaldf = pd.concat([totaldf, df])

        else:
            print('File Szie is Zero:',fin)
    print('total',totaldf)
    for t in tickerlist:
        srdict = {}
        tmpdf = totaldf[totaldf.index.str.contains(t)].sort_index()
        for i in tmpdf.index:
            mi = i.split('.')[:-1]
            mistr = '.'.join(mi)
            
            if mistr in srdict:
                srdict[mistr] += tmpdf.loc[i,'sr']
            else:
                srdict[mistr]  = tmpdf.loc[i,'sr']

        maxkey = max(srdict,key=lambda key: srdict[key])
        paramstr = ','.join(maxkey.split('.')[2:])
        isstr = ','.join([str(round(z,2)) for z in totaldf.loc[maxkey+'.is',] ] )
        osstr = ','.join([str(round(z,2)) for z in totaldf.loc[maxkey+'.os',] ] )
        finalstr = '[' + paramstr +  ']' + '#' + t + ' ' + str(round(srdict[maxkey]/2,2)) + ' is ' + isstr + ';os ' +  osstr 
        print(finalstr)


    if False:
        fout = output_dir + f
        if re.match(r'^.*\..*\.csv',f):
            fout = output_dir + f
        else:
            f_split = f.split('.')
            fout = output_dir + f_split[0]  + '.PO.' + f_split[-1] 
        print('fin',fin)
        print('fout',fout)


def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:i:k:o:zdv",["index_col=","help"])
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
    os.environ['CALTYPE'] = 'XSHG'
    os.environ['DERIVED'] = ''
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-i"):
            input_dir  = a
            if a in ['iv30','dpi','gex','Index','chfrc']:
                os.environ['DERIVED'] = a
        elif o == ('-o'):
            output_dir = a 
        elif o == ('-c'):
            os.environ['CALTYPE'] = a
        elif o == ('-z'):
            zfix = True 
        elif o == ('--index_col') and not os.environ['DERIVED'] == '':
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
    
    start_analysis(input_dir,output_dir,index_col,zfix)


if __name__ == '__main__':
    main()

