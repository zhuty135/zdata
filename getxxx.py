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


from os.path import isfile,join
def file_filter(f):
    if f[-4:] in ['.csv'] and not re.match(r'tmp/.*', f):
        return True
    else:
        return False

def specialstr(maxkey,spp):
    slist = spp.split('.')
    if True: 
        plist02 = maxkey.split(spp)
        p0 =  plist02[0].split('.')
        p1 =  plist02[1].split('.')
        
        p0list = [x for x in p0 if x != '']
        p1list = [x for x in p1 if x != '']
        flat_list = [item for sublist in [p0list[2:], [spp], p1list] for item in sublist]
        paramstr = ','.join(flat_list)

    return(paramstr)

def generate_key(t,maxkey,totaldf,srdict):
    #print(maxkey)
    if re.match(r'.*\.0\.2.*$',maxkey):
        paramstr = specialstr(maxkey,'0.2')
    elif re.match(r'.*\.1\.2.*$',maxkey):
        paramstr = specialstr(maxkey,'1.2')
    elif re.match(r'.*\.2\.4.*$',maxkey):
        paramstr = specialstr(maxkey,'2.4')
    elif re.match(r'.*\.\..*',maxkey):
        plist = maxkey.split('.')
        plist.remove('')
        
        params = [ str(round(int(i)/10,1)) if i in ['5'] else i for i in plist]
        
        plist =[ x for x in params if x != '' ] 
        paramstr = ','.join(plist[2:])
        
    else:
        plist =[ x for x in maxkey.split('.') if x != '' ] 
        paramstr = ','.join(plist[2:])

    isstr = ','.join([str(round(z,2)) for z in totaldf.loc[maxkey+'.is',] ] )
    osstr = ','.join([str(round(z,2)) for z in totaldf.loc[maxkey+'.os',] ] )
    finalstr = t + '=[' + paramstr +  ']' + '#' + maxkey + ' ' + str(round(srdict[maxkey]/2,2)) + ' is ' + isstr + ';os ' +  osstr 
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

    tickerlist = []
    [ tickerlist.append('.'.join(f.split('.')[0:2])) for f in csvfiles if not '.'.join(f.split('.')[0:2]) in tickerlist] 
    print(tickerlist)
    for t in tickerlist:
        if not os.environ['TICKER'] == '' and not t == os.environ['TICKER']:
            continue
        srdict = {}
        tmpdf = totaldf[totaldf.index.str.contains(t)].sort_index()
        for i in tmpdf.index:
            mi = i.split('.')[:-1]
            mistr = '.'.join(mi)
            
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
        maxkey = max(srdict,key=lambda key: srdict[key])
        fs = generate_key(t,maxkey,totaldf,srdict)
        del srdict[maxkey]
        print(fs)

        maxkey1 = max(srdict,key=lambda key: srdict[key])
        fs1 = generate_key(t,maxkey1,totaldf,srdict)
        del srdict[maxkey1]
        print(fs1)

        maxkey2 = max(srdict,key=lambda key: srdict[key])
        fs2 = generate_key(t,maxkey2,totaldf,srdict)
        del srdict[maxkey2]
        print(fs2)


        maxkey3 = max(srdict,key=lambda key: srdict[key])
        fs3 = generate_key(t,maxkey3,totaldf,srdict)
        del srdict[maxkey3]
        print(fs3)

        maxkey4 = max(srdict,key=lambda key: srdict[key])
        fs4 = generate_key(t,maxkey4,totaldf,srdict)
        del srdict[maxkey4]
        print(fs4)

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

