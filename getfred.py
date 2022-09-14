#!/usr/bin/env python3
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from fredapi import Fred
fred = Fred(api_key='7a5cfb682a9c5c2ae2768b8b432171a8')
import requests
from io import StringIO
import time
import datetime as dtt
pd.set_option('display.max_rows',20)

data_wdir = '/work/jzhu/output/'

# 1、 fred数据
def fetch_fred(tickers,oflag=False):
    rdict = {}
    for t in tickers:
        rdata = fred.get_series(t)
        rdict[t] = rdata 
        if oflag:
            opath = '/work/jzhu/output/fred/' + t + '.csv'
            rdata.to_csv(opath)
            print('output to:', opath)
    #print(rdict)
    rdf = pd.DataFrame.from_dict(rdict,orient='columns')
    return(rdf)

tickers = ['BAMLH0A1HYBB','BAMLC0A4CBBB','T10Y2Y','DGS10','DGS2','CUSR0000SEEB','CPIMEDSL','INDPRO','PI','RETAILIRSA','MRTSIR452USS']
ddf = fetch_fred(tickers,oflag=True)
print(ddf)
tickers = ['TOTCI','TOTCI','CONSUMER','ICSA','CCSA','UMCSENT','HSN1F','USREC','TOTALSA','PAYEMS','MRTSSM44X72USS','GACDFSA066MSFRBPHI','HOUST','DGORDER']

st = datetime.date(2010, 1, 8)

def  fetch_cboe(tickers,oflag=False):
    rdict = {}
    for t in tickers:
        flink = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/' + t + '_History.csv'
        print(flink)
        rdata = pd.read_csv(flink)
        #print(rdata)
        rdata.set_index('DATE',inplace=True)
        if oflag:
            opath = '/work/jzhu/output/cboe/' + t + '.csv'
            rdata.to_csv(opath)
            print('output to:', opath)
        #print(rdata) 
    
        if t in  ['COR3M','VIX','VXFXI','VXEWZ','VXEFA','VXEEM']:
            rdict[t] = rdata['CLOSE'][-1000:]
        else:
            rdict[t] = rdata[t][-1000:]
    print(rdict)
    rdf = pd.DataFrame.from_dict(rdict,orient='columns')
    return(rdf)
tickers = ['COR3M','VIX','SKEW','VVIX','VXFXI','OVX','EVZ','GVZ','VXEWZ','VXEFA','VXEEM']
cdf = fetch_cboe(tickers,oflag=True)
print(cdf.iloc[-1,:])

assert(0)

    
a99=fred.get_series('PAYEMS')
a9=a99.copy()
for i in range(0,len(a9)):
    if i==0:
        a9[i]=0
    else:
        a9[i]=a99[i]-a99[i-1]
a10=fred.get_series('MRTSSM44X72USS')
a11=fred.get_series('GACDFSA066MSFRBPHI')
a12=fred.get_series('HOUST')
a13=fred.get_series('DGORDER')
now=time.strftime('%Y-%m',time.localtime(time.time()))+'-01'
# a14=w.edb("G0002323", "2020-01-01", now,"Fill=Previous").Data[0]
a15=fred.get_series('NCBCMDPMVCE')
a16=fred.get_series('CP')
a177=fred.get_series('GDPC1')
minlen=min(len(a177),len(a16))
a17=a16.copy()
for i in range(0,minlen):
    a17[i]=a16[i]/a177[i]*100
a18=fred.get_series('DRTSCILM')
a19=fred.get_series('QBPLNTLNNCUR')

#制表
dflist=[]
for i in [a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a15,a16,a17,a18,a19]: #,a14
    if i is a17 or i is a19:
        latest_data = str(format(i[-1],'.2f'))+' 日期:'+str(i.index[-1])[0:10]
        dflist.append([latest_data,format(i[-2],'.2f'),format(i[-3],'.2f'),format(i[-4],'.2f')])
    elif i is a3 or i is a4:
        latest_data = str(format(i[-1]/1000,'.0f'))+' 日期:'+str(i.index[-1])[0:10]
        dflist.append([latest_data,format(i[-2]/1000,'.0f'),format(i[-3]/1000,'.0f'),format(i[-4]/1000,'.0f')])
    else:
        latest_data = str(format(i[-1],'.0f'))+' 日期:'+str(i.index[-1])[0:10]
        dflist.append([latest_data,format(i[-2],'.0f'),format(i[-3],'.0f'),format(i[-4],'.0f')])
df = pd.DataFrame(dflist, index=['工商业贷款（十亿）','消费者贷款总额（十亿）','初领失业金人数（千）','续领失业金人数（千）','消费者信心指数',
'新屋销售（千）','经济衰退指标','总汽车销量（百万）','非农就业数（千）','零售总额（百万）','费城联储制造业指数','新房开工（千）',
'耐用消费品（百万美元）','企业杠杆率%','企业利润（十亿）','企业利润占GDP%','银行对大中企业贷款收紧比例%','不良率%'],
                  columns=['最新','T-1','T-2','T-3'])

df.to_excel(data_wdir+'usmarco.xlsx')




# 2、市场崩盘数据
mylist=[]
vix=pd.read_csv('https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv')    
mylist.append(vix['CLOSE'].tolist()[-1])
skew=pd.read_csv('https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv')
mylist.append(skew['SKEW'].tolist()[-1])
vvix=pd.read_csv('https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv')
mylist.append(vvix['VVIX'].tolist()[-1])
mylist.append(0)
url = 'https://squeezemetrics.com/monitor/download/1611/SPX.csv'
username = '3037'
password = 'cfcg048'
response=requests.get(url, auth=(username, password))
df1 = pd.read_csv(StringIO(response.text))
mylist.append(df1['DIX'].tolist()[0])
mylist.append(df1['GEX'].tolist()[0])
mylist

def p(mylist):
    count = 0
    if mylist[0] > 30:
        count += 1
    if mylist[1] > 130:
        count += 1
    if mylist[2] > 110:
        count += 1
    if mylist[3] < 0:
        count += 1
    if mylist[4] < 45:
        count += 1
    if mylist[5] < 0:
        count += 1
    return count/6
        
mylist.append(p(mylist))
df2 = pd.DataFrame({'latest':mylist,'standard':[30,130,110,0,45,0,np.nan]},index=['VIX','SKEW','VVIX','0','DIX','GEX','p_value'])
df2.to_excel(data_wdir+'mktcrashdata.xlsx')

