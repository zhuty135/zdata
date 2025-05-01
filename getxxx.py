#!/usr/local/anaconda3/bin/python3.9
import re
import pandas as pd
import numpy as np
import sys
import os
import pwd
uname = pwd.getpwuid(os.getuid()).pw_name
sys.path.append('/work/'+uname+ '/project/zlib/')

from zutils import get_prev_business_date, get_business_date_list

mysplit='_'

from os.path import isfile,join
def file_filter(f):
    if f[-4:] in ['.xxx'] and not re.match(r'tmp/.*', f):
        return True
    else:
        return False

def generate_key(t,maxkey,totaldf,srdict):
    #print('maxkey',maxkey)
    if True: 
        #plist =[ x for x in maxkey.split('-') if x != '' ] 
        #plist =[ x for x in maxkey.split('_') ] 
        plist =[ x for x in maxkey.split(mysplit) ] 
        
        #print('plist',plist)
        paramstr = ','.join(plist[1:])

    #print('paramstr',paramstr)
    #print('maxkey',maxkey)
    #print(totaldf.index)
    #print(maxkey + mysplit + 'is')
    isstr = ','.join([str(round(z,2)) for z in totaldf.loc[maxkey + mysplit + 'is',] ] )
    osstr = ','.join([str(round(z,2)) for z in totaldf.loc[maxkey + mysplit + 'os',] ] )
    finalstr = t + '=[' + paramstr +  ']' + '#' + maxkey + ' ' + os.environ['FILTERTYPE'] + '=' + str(round(srdict[maxkey]/2,2)) + ' is ' + isstr + ';os ' +  osstr 
    return(finalstr)

def start_analysis(input_dir,output_dir,index_col,zfix):
    input_dir = '/work/jzhu/output/' + input_dir
    files = [f for f in os.listdir(input_dir) if isfile(join(input_dir, f))]
    csvfiles = list(filter(file_filter,files))
    print(input_dir,output_dir)
    #print(csvfiles)
    os.makedirs(output_dir,exist_ok=True)


    totaldf = pd.DataFrame()
    
    for f in csvfiles:
        t = f.split('.xxx')[0]
        fin = input_dir + f
        print(fin)
        fsize = os.path.getsize(fin)
        fzero = os.stat(fin).st_size 
        if not fsize == 0 or not fzero == 0:
            df = pd.read_csv(fin,sep = '\s+',names=['sr','ret','vol','dd','txns'])
            totaldf = pd.concat([totaldf, df])
            totaldf.fillna(0,inplace=True)

        else:
            #print(csvfiles)
            csvfiles.remove(f)
            #print(csvfiles)
            print('File Szie is Zero:',fin)
    totaldf.dropna(inplace=True,axis=0)
    print('total',totaldf)

    tickerlist = []
    #[ tickerlist.append('.'.join(f.split('.')[0:3])) if re.match(r'*csab*',f) else tickerlist.append('.'.join(f.split('.')[0:2])) for f in csvfiles if not '.'.join(f.split('.')[0:2]) in tickerlist] 
    [tickerlist.append('.'.join(f.split('.')[0:2])) for f in csvfiles if not '.'.join(f.split('.')[0:2]) in tickerlist] 
    print(tickerlist)
    for t in tickerlist:
        if not os.environ['TICKER'] == '' and not t == os.environ['TICKER']:
            continue
        srdict = {}
        tmpdf = totaldf[totaldf.index.str.contains(t)].sort_index()
        for i in tmpdf.index:
            mi = i.split(mysplit)[0:-1]
            #print(mi)
            mistr = mysplit.join(mi)
            #print(mistr)
            if mistr in srdict:
                if os.environ['FILTERTYPE'] == 'sr':
                    srdict[mistr] += tmpdf.loc[i,'sr']
                    #print('checksr',i,tmpdf.loc[i,'sr'])
                elif os.environ['FILTERTYPE'] == 'dd':
                    srdict[mistr] += tmpdf.loc[i,'ret'] /  np.abs(tmpdf.loc[i,'dd'])
                else:
                    assert(0)
            else:
                if os.environ['FILTERTYPE'] == 'sr':
                    srdict[mistr] = tmpdf.loc[i,'sr']
                    #print('checksr',i,tmpdf.loc[i,'sr'])
                elif os.environ['FILTERTYPE'] == 'dd':
                    srdict[mistr] = tmpdf.loc[i,'ret'] /  np.abs(tmpdf.loc[i,'dd'])
                else:
                    assert(0)

        print(t)
        #print(srdict)
        maxkey = max(srdict,key=lambda key: srdict[key])
        fs = generate_key(t,maxkey,totaldf,srdict)
        del srdict[maxkey]
        print(fs)

        maxkey1 = max(srdict,key=lambda key: srdict[key])
        fs1 = generate_key(t,maxkey1,totaldf,srdict)
        del srdict[maxkey1]
        print(fs1)

def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"t:f:c:i:k:o:zdv",["index_col=","help"])
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
    os.environ['TICKER'] = ''
    os.environ['FILTERTYPE'] = 'sr' 

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
        elif o == ('-t'):
            os.environ['TICKER'] = a 
        elif o == ('-f'):
            os.environ['FILTERTYPE'] = a
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

