#!/usr/local/anaconda3/bin/python3.9
import pandas as pd
import numpy as np
import os

def calc_beta():
    #rawtikcers=['SW857411.PO','000827.SH','SW857372.PO','SW857373.PO','SW857355.PO','SW850854.PO','830933.NQ','SW857321.PO','SW857641.PO','SW851523.PO','SW857371.PO','SW850831.PO','SW857451.PO','SW801750.PO','SW850817.PO','SW801765.PO']
    #rawtikcers=['000988.SH','000990.SH','000991.SH','000992.SH','000993.SH','000994.CSI','000995.CSI','000803.CSI','000941.CSI','SW850812.PO' ,'SW851563.PO' ,'000827.SH' ,'SW857372.PO' ,'SW857373.PO' ,'SW857355.PO' ,'SW850854.PO' ,'SW857321.PO' ,'SW857641.PO' ,'SW851523.PO' ,'SW801770.PO' ,'SW857371.PO' ,'SW850831.PO' ,'SW850833.PO' ,'SW857451.PO' ,'SW801750.PO' ,'SW857661.PO' ,'SW850817.PO' ,'SW801765.PO','930709.CSI','931380.CSI','930931.CSI']
    rawtikcers=['000988.SH','000990.SH','000991.SH','000992.SH','000993.SH','000994.CSI','000995.CSI','000803.CSI','000941.CSI','SW850812.PO' ,'SW851563.PO' ,'000827.SH' ,'SW857372.PO' ,'SW857373.PO' ,'SW857355.PO' ,'SW850854.PO' ,'SW857321.PO' ,'SW857641.PO' ,'SW851523.PO' ,'SW801770.PO' ,'SW857371.PO' ,'SW850831.PO' ,'SW850833.PO' ,'SW857451.PO' ,'SW801750.PO' ,'SW857661.PO' ,'SW850817.PO' ,'SW801765.PO','930709.CSI','931380.CSI','930931.CSI','3800.HK','3993.HK','830933.NQ']
    #codes=['000300.SH','000905.SH','000852.SH','000016.SH','159915.SZ']
    codes=['000300.SH','000905.SH','000852.SH']
    rawtikcers = rawtikcers + codes
    
    df=pd.DataFrame()
    
    #for t in [rawtikcers,codes]:
    #    for code in t:
    for code in rawtikcers:
        if True:
            path_file='/work/jzhu/data/pol/moredata/'+code+".csv"
            x=pd.read_csv(path_file)
            x=x[x['adjusted']>0]
            x=x.set_index('date')
            x=x['adjusted']
            x=x/x.shift(1)-1
            df[code]=x
    print(['constant']+ codes)
    coefdf=pd.DataFrame(np.zeros([len(rawtikcers),len(codes)+1]),index=rawtikcers,columns=['constant']+ codes)
    
    for t in rawtikcers:
        df=df[df.index>='2018-08-03']
        #df=df[-500:]
        x=df.loc[:,codes]
        x.insert(0,'constant',1)
        x=np.array(x)
        y=df.loc[:,t]
        y=np.array(y)
        reg= np.linalg.lstsq(x, y,rcond=None)
        coef=reg[0]
        print(coef)
        coefdf.loc[t,:]= coef#,columns=[['constant']+ codes])
    
    print(coefdf)
    return(coefdf)
    
def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:m:w:s:e:t:r:l:vo",["mode=", "help"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)
    os.environ['OFLAG'] = 'False'
    os.environ['ASSETTYPE'] = 'hzsa' 
    os.environ['RUNMODE'] = 'beta'

    for o, a in opts:
        if o == "-v":
            os.environ['SGRIDVERBOSE']= 'True'
        elif o in ("-m","--mode"):
            os.environ['RUNMODE'] = a
        elif o in ("-w"):
            os.environ['VMFLAG'] = a
        elif o in ("-s"):
            start = datetime(int(a[:4]),int(a[4:6]),int(a[6:8]), 0, 0, 0, 0, pytz.utc)
        elif o in ("-e"):
            os.environ['ENDDT'] = a
        elif o in ("-t"):
            os.environ['ASSETTYPE'] = a
        elif o in ("-l"):
            os.environ['LONGONLY'] = a
        elif o in ("-o"):
            os.environ['OFLAG'] = 'True'
   

    codf = calc_beta()
    
    if eval(os.environ['OFLAG'] ):
        ofile = '/work/jzhu/output/ql/zmpa/' + os.environ['ASSETTYPE'] +'.'  + os.environ['RUNMODE'] + '.csv'
        print(ofile)
        codf.to_csv(ofile)



if __name__ == '__main__':
    main()

