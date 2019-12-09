#!/bin/bash
ARGS=`getopt -o d: -l "p:,help" -- "$@"`
eval set -- "${ARGS}"


opt_flag=false
fut_flag=false
idx_flag=false
nav_flag=false
d_type=''
while true;
do
    case "$1" in
        -d)
            d_type=$2
            shift 2
            ;;
        --help)
            echo "i am help info"
            exit 0
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Internal error!"
            exit 1
            ;;
    esac
done

echo "working on $d_type"

if [ $d_type == 'fut' ]; then
    /work/jzhu/project/zdata/tutosql.py -d fut -n -5 -a -o > /tmp/tu_fut.log 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d fut -o> /tmp/cbcsv_fut.log 2>&1

    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/dce/daily/ > /tmp/fdp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/zce/daily/ > /tmp/fzp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/shf/daily/ > /tmp/fsp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/ine/daily/ > /tmp/fip.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/cfx/daily/ > /tmp/fcp.log 2>&1

elif [ $d_type == 'opt' ]; then
    /work/jzhu/project/zdata/tutosql.py -d opt -n -5 -a -o > /tmp/tu_opt.log 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d opt -o> /tmp/cbcsv_opt.log 2>&1

    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/opt/sql/dce/daily/ > /tmp/odp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/opt/sql/zce/daily/ > /tmp/ozp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/opt/sql/shf/daily/ > /tmp/osp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/opt/sql/sse/daily/ > /tmp/ocp.log 2>&1

elif [ $d_type == 'fund' ]; then
    /work/jzhu/project/zdata/tutosql.py -d fund -n -5 -a -o > /tmp/tu_fund.log 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d fund -o> /tmp/cbcsv_fund.log 2>&1

elif [ $d_type == 'index' ]; then
    /work/jzhu/project/zdata/tutosql.py -d index -n -5 -a -o > /tmp/tu_index.log 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d index -o> /tmp/cbcsv_index.log 2>&1

elif [ $d_type == 'ci' ]; then
    /work/jzhu/project/finger/index/co_index.py > /tmp/ci.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i Index --index_col='time' > /tmp/cipol.log 2>&1

    scp -r /work/jzhu//data/pol/Index/*.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ #> /tmp/ciscp.log 2>&1

else
    echo "WARNING: wrong data type $d_type"
fi

exit 0







