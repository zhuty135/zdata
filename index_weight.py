#!/usr/bin/python3

###############################################################################
###############################################################################
##### extract the weight of index from Tushare(HS300 index and ZZ500 index)
##### Teng Ma     2019-08-05

 
###############################################################################
###############################################################################
#### import the API with the database Tushare 
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


###############################################################################
###############################################################################
#### import the necessary packages
import pandas as pd
import numpy as np
import pprint as pprint



###############################################################################
###############################################################################
#### set the constants
index = "300"                    ## choose the index code: 300 or 905
start_date = "20190101"          ## the initial date
end_date = "20190801"            ## the end date



###############################################################################
###############################################################################
#### extract the weight data for the index
#index = "000" + index_code + ".SH"
#print(index)
 

def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"i:h",["index=", "help"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)

    #dkey = 'fund_nav'
    #index = "000" + index_code + ".SH"
    #print(index)

    for o, a in opts:
        if o in ("-i","--index"):
            index = a
        else:
            assert False, 'unhandled option'
    
    index_code = index#"000" + index + ".SH"
    print(index_code)

    #### extract the data from Tushare
    IndexData = pro.index_weight(index_code = index_code, start_date = start_date, end_date = end_date)
    df = pro.index_weight(index_code='000016.SH', start_date='20180901', end_date='20180930')
    print('df',df)


    
    print(index_code,start_date,end_date,IndexData.head())
    
    #### ouput the weight data
    IndexData.to_csv("./output/" + index + "weight.csv", index = False,header = True)
    print("The weight of " + index_code + " have been download")


if __name__ == "__main__":
   main()



