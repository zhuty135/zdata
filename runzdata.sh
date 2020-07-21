#!/bin/bash
ARGS=`getopt -o d: -l "p:,help" -- "$@"`
eval set -- "${ARGS}"


opt_flag=false
fut_flag=false
idx_flag=false
nav_flag=false
d_type=''
edate=`date +"%Y-%m-%d"`
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

echo "working on $d_type, $edate"

if [ $d_type == 'fut' ]; then
    /work/jzhu/project/zdata/tutosql.py -d fut -n -50 -a -o -e $edate > /tmp/tu_fut.log 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d fut -o -e $edate > /tmp/cbcsv_fut.log 2>&1

    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/dce/daily/ > /tmp/fdp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/zce/daily/ > /tmp/fzp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/shf/daily/ > /tmp/fsp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/ine/daily/ > /tmp/fip.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fut/sql/cfx/daily/ > /tmp/fcp.log 2>&1

elif [ $d_type == 'opt' ]; then
    /work/jzhu/project/zdata/tutosql.py -d opt -n -50 -a -o > /tmp/tu_opt.log 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d opt -o> /tmp/cbcsv_opt.log 2>&1

    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/opt/sql/dce/daily/ > /tmp/odp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/opt/sql/zce/daily/ > /tmp/ozp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/opt/sql/shf/daily/ > /tmp/osp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/opt/sql/sse/daily/ > /tmp/ocp.log 2>&1

elif [ $d_type == 'fund' ]; then
    /work/jzhu/project/zdata/tutosql.py -d fund -n -5 -a -o > /tmp/tu_fund.log 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d fund -o> /tmp/cbcsv_fund.log 2>&1

    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fund/sql/e/daily/ > /tmp/fund_e.log 2>&1

elif [ $d_type == 'fund_nav' ]; then
    /work/jzhu/project/zdata/tutosql.py -d fund_nav -n -5 -a -o > /tmp/tu_fund_nav.log 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d fund_nav -o> /tmp/cbcsv_fund.log 2>&1

    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/fund_nav/sql/o/daily/ > /tmp/fund_nav_o.log 2>&1


elif [ $d_type == 'index' ]; then
    /work/jzhu/project/zdata/tutosql.py -d index -n -5 -a -o > /tmp/tu_index.log 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d index -o> /tmp/cbcsv_index.log 2>&1

    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/index/sql/szse/daily/ > /tmp/index_sz.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/index/sql/sse/daily/ > /tmp/index_sh.log 2>&1

elif [ $d_type == 'ci' ]; then
    /work/jzhu/project/finger/index/co_index.py  > /tmp/ci.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i Index --index_col='time' > /tmp/cipol.log 2>&1

    scp -rp /work/jzhu//data/pol/Index/*.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ 
    scp -rp /work/jzhu/input/Index/ jzhu@106.14.226.83:/work/shared/raw/

elif [ $d_type == 'pi' ]; then
    scp -rp /work/jzhu/output/ql/mpa/*csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ 
    scp -rp /work/jzhu/output/dm/*csv  jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ 


elif [ $d_type == 'calm' ]; then
    /work/jzhu/project/ql/script/calm.py > /work/shared/daily/log/calm.log 2>&1
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/calm/CALM.cfpa >> /work/shared/daily/log/calm.log 2>&1
    scp -rp /work/shared/daily/ql/calm/CALM.wsign.cfpa.2020* jzhu@106.14.226.83:/work/shared/daily/ql/calm/ > /tmp/calm_scp.log


elif [ $d_type == 'mpa' ]; then
    /work/jzhu/project/ql/script/mpa.py > /work/shared/daily/log/mpa.log 2>&1
    scp -rp /work/shared/daily/ql/mpa/MPA.wsign.cfpa.2020* jzhu@106.14.226.83:/work/shared/daily/ql/mpa/ 

    /work/jzhu/project/ql/script/zmpa.py -t ifpa > /work/shared/daily/log/zmpa.ifpa.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t tfpa > /work/shared/daily/log/zmpa.tfpa.log 2>&1
    scp -rp /work/shared/daily/ql/zmpa/* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 

    /work/jzhu/project/ql/script/bondtech.py > /tmp/bondtech.log 2>&1
    scp -rp /work/shared/daily/ql/mpa/MPA.wsign.t.2020* jzhu@106.14.226.83:/work/shared/daily/ql/mpa/

    #/work/jzhu/project/ql/script/stocktechih.py > /tmp/stocktechih.log 2>&1
    #/work/jzhu/project/ql/script/stocktech300.py > /tmp/stocktechif.log 2>&1
    #/work/jzhu/project/ql/script/stocktech500.py > /tmp/stocktechic.log 2>&1
    #scp -rp /work/shared/daily/ql/mpa/MPA.wsign.i*.2020* jzhu@106.14.226.83:/work/shared/daily/ql/mpa/

elif [ $d_type == 'gbw' ]; then
    /work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single -t cfpa > /tmp/mpa_cfpa_sgrid.log 
    /work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single -t cfsa > /tmp/mpa_cfsa_sgrid.log 
    #/work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single  -i CFCGSA.PO > /tmp/mpa_sgrid_SC.log 
    #/work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single  -i CFNMSA.PO > /tmp/mpa_sgrid_NM.log 
    #/work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single  -i CFCMSA.PO > /tmp/mpa_sgrid_CM.log 
    #/work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single  -i CFFMSA.PO > /tmp/mpa_sgrid_FM.log 
    #/work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single  -i CFPMSA.PO > /tmp/mpa_sgrid_PM.log 
    #/work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single  -i CFECSA.PO > /tmp/mpa_sgrid_EC.log 
    #/work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single  -i CFOPSA.PO > /tmp/mpa_sgrid_OP.log 
    #/work/jzhu/project/ql/script/sgrid.py -m ql/mpa/single  -i CFCICA.PO > /tmp/mpa_sgrid_CI.log 


    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/mpa/single  > /tmp/mpa_pk_to_csv.log 

    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t ifpa > /tmp/zmpa_ifpa_zgrid.log 
    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t tfpa > /tmp/zmpa_tfpa_zgrid.log 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/single  > /tmp/zmpa_pk_to_csv.log 

    /work/jzhu/project/slib/script/bbw.py -t cfpa -w ewvtb2 > /tmp/mpa_bbw_cfpa.log 
    /work/jzhu/project/slib/script/bbw.py -t cfsa -w ewvtb2 > /tmp/mpa_bbw_cfsa.log 

    /work/jzhu/project/slib/script/bbw.py -t if > /tmp/mpa_bbw_eq.log 
    /work/jzhu/project/slib/script/bbw.py -t tf > /tmp/mpa_bbw_eq.log 

    scp -rp /work/shared/daily/slib/bbx/mpa.absw.*.2020* jzhu@106.14.226.83:/work/shared/daily/slib/bbx/ >> /tmp/mpa_scp.log

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/bbx/mpa.cfpa.ewvtb2 > /tmp/mpa_cfpa_ewvt.log 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/bbx/mpa.cfsa.ewvtb2 > /tmp/mpa_cfsa_ewvt.log 

elif [ $d_type == 'scp' ]; then

    scp -rp /work/jzhu/output/ql/mpa/single/*.MPA.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/
    scp -rp /work/jzhu/output/ql/mpa/single/*.MPA.csv jzhu@106.14.226.83:/work/shared/daily/ql/mpa/single/

    scp -rp /work/jzhu/output/slib/bbx/mpa.ewvt.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ >> /tmp/mpa_scp.log 
    scp -rp /work/jzhu/output/slib/bbx/mpa.ewvt.csv jzhu@106.14.226.83:/work/jzhu/input/se2018/daily/ >> /tmp/mpa_scp.log 





else
    echo "WARNING: wrong data type $d_type"
fi 
exit 0







