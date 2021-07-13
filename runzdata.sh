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
    /work/jzhu/project/zdata/tutosql.py -d fut -n -15 -a -o -e $edate > /tmp/tu_fut.log.$edate 2>&1
    /work/jzhu/project/zdata/sqltocsv.py  -d fut -o -e $edate > /tmp/cbcsv_fut.log.$edate 2>&1

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
    /work/jzhu/project/finger/zlib/zutils.py > /tmp/generate_new _cal_file.ci.log
    /work/jzhu/project/finger/index/co_index.py  > /tmp/ci.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i Index --index_col='time' > /tmp/cipol.log 2>&1

    scp -rp /work/jzhu//data/pol/Index/*.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ 
    scp -rp /work/jzhu/input/Index/ jzhu@106.14.226.83:/work/shared/raw/

elif [ $d_type == 'pi' ]; then
    scp -rp /work/jzhu/output/ql/mpa/*csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ 
    scp -rp /work/jzhu/output/dm/*csv  jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ 


elif [ $d_type == 'calm' ]; then
    /work/jzhu/project/ql/script/calm.py > /work/shared/daily/log/calm.log 2>&1
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/calm/CALM.cfpa > /work/shared/daily/log/calm.clog 2>&1
    scp -rp /work/shared/daily/ql/calm/CALM.wsign.cfpa.2021* jzhu@106.14.226.83:/work/shared/daily/ql/calm/ > /tmp/calm_scp.log


elif [ $d_type == 'mpa' ]; then
    /work/jzhu/project/ql/script/mpa.py > /work/shared/daily/log/mpa_cfpa.log 2>&1
    /work/jzhu/project/ql/script/mpa.py -t cfsa > /work/shared/daily/log/mpa_cfsa.log 2>&1
    #scp -rp /work/shared/daily/ql/mpa/MPA.wsign.cf*a.2020* jzhu@106.14.226.83:/work/shared/daily/ql/mpa/ 

    #/work/jzhu/project/ql/script/bondtech.py > /tmp/bondtech.log 2>&1
    #scp -rp /work/shared/daily/ql/mpa/MPA.wsign.t.2020* jzhu@106.14.226.83:/work/shared/daily/ql/mpa/

    #/work/jzhu/project/ql/script/stocktechih.py > /tmp/stocktechih.log 2>&1
    #/work/jzhu/project/ql/script/stocktech300.py > /tmp/stocktechif.log 2>&1
    #/work/jzhu/project/ql/script/stocktech500.py > /tmp/stocktechic.log 2>&1
    #scp -rp /work/shared/daily/ql/mpa/MPA.wsign.i*.2020* jzhu@106.14.226.83:/work/shared/daily/ql/mpa/
elif [ $d_type == 'zmpa' ]; then
    /work/jzhu/project/ql/script/zmpa.py -t ifpa -s 20160512 > /work/shared/daily/log/zmpa.ifpa.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t tfpa -s 20160512 > /work/shared/daily/log/zmpa.tfpa.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t cfca > /work/shared/daily/log/zmpa.cfca.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t cfsa > /work/shared/daily/log/zmpa.cfsa.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t cfpa > /work/shared/daily/log/zmpa.cfpa.log 2>&1

    /work/jzhu/project/ql/script/zmpa.py -t cfo2 -m ql/zmpa/LOZMPA -r w > /work/shared/daily/log/lozmpa.cfo2.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t hack -m ql/zmpa/LOZMPA -r w -s 20210130 > /work/shared/daily/log/lozmpa.hack.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t ifpa -m ql/zmpa/LOZMPA > /work/shared/daily/log/lozmpa.ifpa.log 2>&1

    scp -rp /work/shared/daily/ql/zmpa/*2021* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 
    scp -rp /work/shared/daily/ql/zmpa/*2021* jzhu@123.57.60.6:/work/dwhang/project/sit/Shiny/yzpa/
    
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.cfpa > /work/shared/daily/log/zmpa.cfpa.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.cfsa > /work/shared/daily/log/zmpa.cfsa.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.cfca > /work/shared/daily/log/zmpa.cfca.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.ifpa > /work/shared/daily/log/zmpa.ifpa.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.tfpa > /work/shared/daily/log/zmpa.tfpa.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/LOZMPA.cfo2 > /work/shared/daily/log/zmpa.cfo2.clog 2>&1 &

    scp -rp /work/jzhu/output/ql/zmpa/ZMPA.*.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/

elif [ $d_type == 'dmw' ]; then
    /work/jzhu/project/slib/script/mw.py -s 20210630 -t copc -o net  > /work/shared/daily/log/mw.net.log 2>&1 
    /work/jzhu/project/slib/script/mw.py -s 20210630 -t copc -o sep  > /work/shared/daily/log/mw.sep.log 2>&1 
    scp -rp /work/shared/daily/slib/mmw/*202* jzhu@106.14.226.83:/work/shared/daily/slib/mmw/ 2>&1  
elif [ $d_type == 'dlw' ]; then
    /work/jzhu/project/ql/script/lw.py -m ql/ssl/ls -t coss -r w -s 20180512 > /work/shared/daily/log/lw.ls.coss.log 2>&1 

elif [ $d_type == 'drw' ]; then
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t cfpa -l 10
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t cfpa -l 20
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t cfpa -l 40
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t shsz -l 10
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t shsz -l 20
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t shsz -l 40
    /work/jzhu/project/slib/script/rw.py -m slib/lws/lo -t coxm -r w -s 20180512 > /work/shared/daily/log/rw.lo.coxm.log 2>&1 
    /work/jzhu/project/slib/script/rw.py -m slib/lws/so -t coxm -r w -s 20180512 > /work/shared/daily/log/rw.so.coxm.log 2>&1 

    /work/jzhu/project/slib/script/rw.py -m slib/lws/lo -t hzxm -r w -s 20180512 > /work/shared/daily/log/rw.lo.hzxm.log 2>&1 



elif [ $d_type == 'grw' ]; then
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t spgs -l 10
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t spgs -l 20
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t spgs -l 40
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t idxetf -l 10
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t idxetf -l 20
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t idxetf -l 40
    /work/jzhu/project/slib/script/rw.py -m slib/lws/lo -t idxetf -r w -s 20191212 > /work/shared/daily/log/rw.lo.idxetf.log 2>&1 

    /work/jzhu/project/slib/script/rwgrid.py -m slib/lws/single -t secetf  -l lo -x 4  > /tmp/sec.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/lws/single -t idxetf  > /tmp/idx.log 2>&1 &



elif [ $d_type == 'iv' ]; then
    scp -rp 123.57.60.6:/work/jzhu/input/iv/*.csv /work/jzhu/input/iv/ > /tmp/iv.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/iv/ > /tmp/iv.pol.log 2>&1
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t iv > /tmp/chaodi_iv.log

    /work/jzhu/project/slib/script/kdj.py -t iv6m   -s 20200905 > /work/shared/daily/log/chaodi_iv6m.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -t iv1m   -s 20200905 > /work/shared/daily/log/chaodi_iv1m.log  2>&1 

elif [ $d_type == 'nh' ]; then
    scp -rp 123.57.60.6:/work/jzhu/input/nh/*.csv /work/jzhu/input/nh/ > /tmp/nh.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/nh/ > /tmp/nh.pol.log 2>&1
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t nh > /tmp/chaodi_nh.log

elif [ $d_type == 'idxetf' ]; then
    #scp -rp 123.57.60.6:/work/jzhu/input/idxetf/*.csv /work/jzhu/input/idxetf/ > /tmp/idxetf.scp.log 
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/idxetf/  > /tmp/idxetf.pol.log 

    /work/jzhu/project/ql/script/zmpa.py -t gloetf -s 20190512 > /work/shared/daily/log/zmpa.gloetf.log 2>&1 
    /work/jzhu/project/ql/script/zmpa.py -t secetf -s 20190512 > /work/shared/daily/log/zmpa.secetf.log 2>&1 
    /work/jzhu/project/ql/script/zmpa.py -t cashetf -s 20190512 > /work/shared/daily/log/zmpa.cashetf.log 2>&1  
    /work/jzhu/project/ql/script/zmpa.py -t idxetf -s 20190512 -m ql/zmpa/ZMPA -r w -l lo > /work/shared/daily/log/zmpa.idxetf.log 2>&1 

    scp -rp /work/shared/daily/ql/zmpa/ZMPA.wsign.gloetf.* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 2>&1  
    scp -rp /work/shared/daily/ql/zmpa/ZMPA.wsign.secetf.* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 2>&1  
    scp -rp /work/shared/daily/ql/zmpa/ZMPA.wsign.cashetf.* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 2>&1 

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.gloetf > /work/shared/daily/log/zmpa.gloetf.clog 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.secetf > /work/shared/daily/log/zmpa.secetf.clog
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.cashetf > /work/shared/daily/log/zmpa.cashetf.clog

    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t gloetf > /work/shared/daily/log/zmpa_gloetf_zgrid.log 2>&1 

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/single -t gloetf > /work/shared/daily/log/zmpa.gloetf.grid.log 

    /work/jzhu/project/slib/script/bby.py -t gloetf -w ew  -m slib/zbw/zmpa -s 20161127 > /tmp/zmpa_zbw_gloetf_ewvtb2.log 


elif [ $d_type == 'idxcom' ]; then
    #scp -rp 123.57.60.6:/work/jzhu/input/global/*.csv /work/jzhu/input/global/ > /tmp/global.scp.log 
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/global/  > /tmp/global.pol.log 

    /work/jzhu/project/ql/script/zmpa.py -t idxcom -s 20190512 > /work/shared/daily/log/zmpa.idxcom.log 2>&1 
    scp -rp /work/shared/daily/ql/zmpa/ZMPA.wsign.idxcom.* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.idxcom > /work/shared/daily/log/zmpa.idxcom.clog  2>&1 

    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t idxcom > /work/shared/daily/log/zmpa_idxcom_zgrid.log 

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/single -t idxcom > /work/shared/daily/log/zmpa.idxcom.grid.log 

elif [ $d_type == 'doch' ]; then
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t cfpa > /tmp/chaodi_cfpa.log
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t shsz > /tmp/chaodi_shsz.log
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t iv > /tmp/chaodi_iv.log
    /work/jzhu/project/slib/script/kdj.py -t cflo -s 20180505 > /work/shared/daily/log/chaodi_cflo.log  2>&1
    /work/jzhu/project/slib/script/kdj.py -t colo -s 20180505 > /work/shared/daily/log/chaodi_colo.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -m slib/sw/so  -t coso -s 20180505 > /work/shared/daily/log/chaodi_coso.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -t hzlo -s 20180505 > /work/shared/daily/log/chaodi_hzlo.log  2>&1 
    scp -rp /work/shared/daily/slib/fsd/jw*2021* jzhu@106.14.226.83:/work/shared/daily//slib/fsd/
elif [ $d_type == 'glch' ]; then
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t idxetf > /tmp/chaodi.log
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t spgs > /tmp/chaodi.log
    /work/jzhu/project/slib/script/kdj.py -t splo -s 20180505 > /work/shared/daily/log/chaodi_splo.log  2>&1
    /work/jzhu/project/slib/script/kdj.py -t fasg -s 20180505 > /work/shared/daily/log/chaodi_fasg.log  2>&1
    /work/jzhu/project/slib/script/kdj.py -t slog -s 20180505 > /work/shared/daily/log/chaodi_slog.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -t morb -s 20180505 > /work/shared/daily/log/chaodi_morb.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -t idxetf -s 20180505 > /work/shared/daily/log/chaodi_idxetf.log  2>&1 
elif [ $d_type == 'zbw' ]; then
    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t cfpa > /work/shared/daily/log/zmpa_cfpa_zgrid.log 2>&1  
    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t cfsa > /work/shared/daily/log/zmpa_cfsa_zgrid.log 2>&1 
    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t ifpa > /work/shared/daily/log/zmpa_ifpa_zgrid.log 2>&1
    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t tfpa > /work/shared/daily/log/zmpa_tfpa_zgrid.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/single -t cfpa > /work/shared/daily/log/zmpa_cfpa_pk_to_csv.log  2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/single -t cfsa > /work/shared/daily/log/zmpa_cfpa_pk_to_csv.log  2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/single -t ifpa > /work/shared/daily/log/zmpa_ifpa_pk_to_csv.log  2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/single -t tfpa > /work/shared/daily/log/zmpa_tfpa_pk_to_csv.log  2>&1 

    /work/jzhu/project/slib/script/bby.py -t yzpa -w ew  -m slib/zbw/zmpa -s 20161127 > /tmp/zmpa_zbw_yzpa_ewvtb2.log 2>&1
    /work/jzhu/project/slib/script/bby.py -t tfpa -w ewvtb2  -m slib/zbw/zmpa -s 20161127 > /tmp/zmpa_zbw_tfpa_ewvtb2.log 2>&1
    /work/jzhu/project/slib/script/bby.py -t ifpa -w ewb2  -m slib/zbw/zmpa  -s 20160512 > /tmp/zmpa_zbw_ifpa_ewb2.log 2>&1 
    /work/jzhu/project/slib/script/bby.py -t cfpa -w ewvt  -m slib/zbw/zmpa -s 20160512 > /tmp/zmpa_zbw_cfpa_ewvt.log 2>&1 
    #/work/jzhu/project/slib/script/bby.py -t cfpa -w ewvt  -m slib/zbw/zmpa > /tmp/zmpa_zbw_cfpa.log 
    #/work/jzhu/project/slib/script/bbw.py -t cfpa -w ewvtb2  -m slib/bbx/zmpa > /tmp/zmpa_bbw_cfpa.log 
    #/work/jzhu/project/slib/script/bby.py -t cfpa -w ewvtb2  -m slib/bbz/zmpa > /tmp/zmpa_bby_cfpa.log 

    scp -rp /work/shared/daily/slib/zbw/zmpa.absw.*.2021* jzhu@106.14.226.83:/work/shared/daily/slib/zbw/ > /tmp/zbw_scp.log
    scp -rp /work/shared/daily/slib/zbw/zmpa.absw.yz*.* jzhu@123.57.60.6:/work/dwhang/project/sit/Shiny/yzpa

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

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/mpa/single -t cfpa > /tmp/mpa_pk_to_csv.log 

    /work/jzhu/project/slib/script/bbw.py -t cfpa -w ewvtb2 > /tmp/mpa_bbw_cfpa.log 
    /work/jzhu/project/slib/script/bbw.py -t cfsa -w ewvtb2 > /tmp/mpa_bbw_cfsa.log 

    /work/jzhu/project/slib/script/bbw.py -t if > /tmp/mpa_bbw_if.log 
    /work/jzhu/project/slib/script/bbw.py -t tf > /tmp/mpa_bbw_tf.log 

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/bbx/mpa.cfpa.ewvtb2 -t cfpa > /tmp/mpa_cfpa_ewvt.log 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/bbx/mpa.cfsa.ewvtb2 -t cfsa > /tmp/mpa_cfsa_ewvt.log 

elif [ $d_type == 'lns' ]; then
    /work/jzhu/project/slib/script/rgrid.py -m slib/lns/single -t cfpa  >  /work/shared/daily/log/lns_cfpa.log 2>&1 
    /work/jzhu/project/slib/script/rgrid.py -m slib/lns/single -t cfir  >  /work/shared/daily/log/lns_cfir.log 2>&1 
    scp -rp /work/shared/daily/slib/lns/single.w.cf* jzhu@106.14.226.83:/work/shared/daily/slib/lns/

elif [ $d_type == 'macro' ]; then
    scp 123.57.60.6:/tmp/macro/*csv /work/jzhu/data/raw
    mv /work/jzhu/data/raw/macroraw.csv /work/jzhu/data/raw/macroraw.csv.$edate
    mv /work/jzhu/data/raw/SHIBOR3M.csv /work/jzhu/data/raw/SHIBOR3M.csv.$edate
    cd /work/jzhu/data/raw
    # cp macro2021-04-01.csv macroraw.csv
    # cp SHIBOR2021-05-01.csv SHIBOR3M.csv

    /work/jzhu/project/zlib/zstats.py -m cal_macro -o > /tmp/cal_macro.log
    cp /work/jzhu/output/macro/ODSCHG.csv /work/jzhu/project/ql/data/
    cp /work/jzhu/output/macro/DEBTCHG_YEAR.csv /work/jzhu/project/ql/data/
    cp /work/jzhu/output/macro/SHIBOR3M.csv /work/jzhu/project/ql/data/
    cp /work/jzhu/output/macro/ADDVALUE_CHG.csv /work/jzhu/project/ql/data/
    cp /work/jzhu/output/macro/M1M2_CHG.csv /work/jzhu/project/ql/data/
    cp /work/jzhu/output/macro/CPI_PPI_CHG.csv /work/jzhu/project/ql/data/

    /work/jzhu/project/ql/script/stockmacroih.py -e $edate


elif [ $d_type == 'scp' ]; then

    scp /work/jzhu/data/pol/Index/*.csv 106.14.226.83:/work/shared/data/pol/Index/
    scp /work/jzhu/data/pol/Index/*.csv 123.57.60.6:/work/jzhu/data/pol/Index/
    scp /work/jzhu/input/index/sql/szse/daily/399300.SZ.csv 123.57.60.6:/work/jzhu/input/index/sql/szse/daily/
    #scp /work/jzhu/data/pol/work/jzhu/input/global/*.csv 123.57.60.6:/work/jzhu/data/pol/work/jzhu/input/global/
    #scp /work/jzhu/data/pol/work/jzhu/input/idxetf/*.csv 123.57.60.6:/work/jzhu/data/pol/work/jzhu/input/idxetf/

    scp -rp /work/jzhu/output/ql/mpa/single/*.MPA.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/
    scp -rp /work/jzhu/output/ql/mpa/single/*.MPA.csv jzhu@106.14.226.83:/work/shared/daily/ql/mpa/single/

    scp -rp /work/jzhu/output/slib/bbx/mpa.ewvt.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ > /tmp/mpa_scp.log 
    scp -rp /work/jzhu/output/slib/bbx/mpa.ewvt.csv jzhu@106.14.226.83:/work/jzhu/input/se2018/daily/ > /tmp/mpa_scp.log 





else
    echo "WARNING: wrong data type $d_type"
fi 
exit 0







