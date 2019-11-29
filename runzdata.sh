#!/bin/bash
ARGS=`getopt -o iu:l:s:e: -l "p:,help" -- "$@"`
eval set -- "${ARGS}"

DATE=$(/usr/bin/python3 -W ignore /work/jzhu/project/gitrepos/finger/script/get_run_date.py)
echo "run date is $DATE"
echo $DATE

/work/jzhu/project/zdata/tutosql.py -d fut -n -5 -a -o > /tmp/tufut.log.$DATE 2>&1

/work/jzhu/project/zdata/sqltocsv.py  -d fut -o> /tmp/cbcsvfut.log.$DATE 2>&1

/work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut_index/sql/dce/daily/ > /tmp/dp.log 2>&1
/work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut_index/sql/zce/daily/ > /tmp/zp.log 2>&1
/work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut_index/sql/shf/daily/ > /tmp/sp.log 2>&1
/work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut_index/sql/ine/daily/ > /tmp/ip.log 2>&1
/work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut_index/sql/cfx/daily/ > /tmp/cp.log 2>&1

/work/jzhu/project/finger/index/co_index.py > /tmp/coindex.log 2>&1

/work/jzhu/script/csvpolish.py -i Index --index_col='time' > /tmp/csvpol.log 2>&1

scp -r /work/jzhu//data/pol/Index/*.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/
