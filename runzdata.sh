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

elif [ $d_type == 'gtaa' ]; then
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/yf > /tmp/yf.log 2>&1
    /work/jzhu/project/ql/script/taa.py -m ql/taa/lo -s 20130512 -g 1 -t glob > /tmp/gtaa.log 2>&1 

elif [ $d_type == 'dtaa' ]; then
    /work/jzhu/project/ql/script/taa.py -m ql/taa/lo -s 20190512 -g 2 -t glob > /tmp/dtaa.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/taa/lo.glob.20 > /tmp/dtaa.pc.log 2>&1 

    scp -rp /work/shared/daily/ql/taa/*glob*2022* user1@8.142.157.170:/work/shared/daily/slib/mmw/
    scp -rp /work/shared/daily/ql/taa/*glob*2022* jzhu@123.57.60.6:/work/dwhang/project/sit/Shiny/yzpa/
    /work/jzhu/project/slib/script/bby.py -t lo.glob.20 -w ewb2  -m ql/taa -s 20160512 > /tmp/zmpa_zbw_glob_ewb2.log 2>&1 



    
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

    scp -rp /work/jzhu/data/pol/index/sql/szse/daily/399300.SZ.csv jzhu@123.57.60.6:/work/jzhu/data/pol/index/sql/szse/daily/
    scp -rp /work/jzhu/data/pol/index/sql/sse/daily/000016.SH.csv jzhu@123.57.60.6:/work/jzhu/data/pol/index/sql/sse/daily/
    scp -rp /work/jzhu/data/pol/index/sql/sse/daily/000905.SH.csv jzhu@123.57.60.6:/work/jzhu/data/pol/index/sql/sse/daily/

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
    scp -rp /work/jzhu/input/Index/ user1@8.142.157.170:/work/shared/raw/

elif [ $d_type == 'user1' ]; then
    scp -rp user1@8.142.157.170:/work/shared/nh/*csv /work/jzhu/input/yf/nh/
    scp -rp user1@8.142.157.170:/work/shared/iv/*csv /work/jzhu/input/yf/iv/
    scp -rp user1@8.142.157.170:/work/shared/moredata/*csv /work/jzhu/input/yf/
    scp -rp user1@8.142.157.170:/work/shared/idxetf/*csv /work/jzhu/input/yf/idxetf/
    scp -rp user1@8.142.157.170:/work/shared/global/*csv /work/jzhu/input/yf/global/
    scp -rp /work/jzhu/input/yf/*  123.57.60.6:/work/jzhu/input/yf/

    #scp -rp /work/jzhu/input/iv/*.csv 123.57.60.6:/work/jzhu/input/iv/ > /tmp/iv.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/iv/ > /tmp/iv.pol.log 2>&1
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t iv > /tmp/chaodi_iv.log

    /work/jzhu/project/slib/script/kdj.py -t iv6m   -s 20200905 > /work/shared/daily/log/chaodi_iv6m.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -t iv1m   -s 20200905 > /work/shared/daily/log/chaodi_iv1m.log  2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/jw/lo.iv6m > /work/shared/daily/log/chaodi_iv6m.clog 2>&1
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/jw/lo.iv1m > /work/shared/daily/log/chaodi_iv1m.clog 2>&1



elif [ $d_type == 'nh' ]; then
    #20220325 scp -rp 123.57.60.6:/work/jzhu/input/nh/*.csv /work/jzhu/input/nh/ > /tmp/nh.log 2>&1
    scp -rp  /work/jzhu/input/nh/*.csv 123.57.60.6:/work/jzhu/input/nh/ > /tmp/nh.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/nh/ > /tmp/nh.pol.log 2>&1
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t nh > /tmp/chaodi_nh.log

elif [ $d_type == 'idxetf' ]; then
    #scp -rp 123.57.60.6:/work/jzhu/input/idxetf/*.csv /work/jzhu/input/idxetf/ > /tmp/idxetf.scp.log 
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/idxetf/  > /tmp/idxetf.pol.log 

    /work/jzhu/project/ql/script/zmpa.py -t gloetf -s 20190512 > /work/shared/daily/log/zmpa.gloetf.log 2>&1 
    /work/jzhu/project/ql/script/zmpa.py -t secetf -s 20190512 > /work/shared/daily/log/zmpa.secetf.log 2>&1 
    /work/jzhu/project/ql/script/zmpa.py -t cashetf -s 20190512 > /work/shared/daily/log/zmpa.cashetf.log 2>&1  

    /work/jzhu/project/ql/script/zmpa.py -t idxetf -s 20150512 -m ql/zmpa/ZMPA -r w -l lo > /work/shared/daily/log/zmpa.idxetf.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.idxetf > /work/shared/daily/log/zmpa.idxetf.clog


elif [ $d_type == 'dplt' ]; then
    /work/jzhu/project/finger/misc/bd.py -l 126 -d 0 -b 0.0001 -o  yzpa > /tmp/bd_126_0.log 2>&1  
    /work/jzhu/project/finger/misc/bd.py -l 126 -d 2 -b 0.0001 -o  yzpa > /tmp/bd_126_2.log 2>&1  
    /work/jzhu/project/finger/misc/bd.py -l 63  -d 0 -b 0.0001 -o  yzpa > /tmp/bd_63_0.log 2>&1  
    /work/jzhu/project/finger/misc/bd.py -l 63  -d 2 -b 0.0001 -o  yzpa > /tmp/bd_63_2.log 2>&1  

    /work/jzhu/project/finger/misc/bd.py -l 63  -d 0 -b 0.0001 -o yzpa  -t pxnh > /tmp/bd_pxnh_63_2.log 2>&1
    /work/jzhu/project/finger/misc/bd.py -l 63  -d 2 -b 0.0001 -o yzpa  -t pxnh > /tmp/bd_pxnh_63_2.log 2>&1
    /work/jzhu/project/finger/misc/bd.py -l 126 -d 0 -b 0.0001 -o yzpa  -t pxnh > /tmp/bd_pxnh_126_0.log 2>&1
    /work/jzhu/project/finger/misc/bd.py -l 126 -d 2 -b 0.0001 -o yzpa  -t pxnh > /tmp/bd_pxnh_126_2.log 2>&1

    /work/jzhu/project/finger/misc/bd.py -l 63  -d 0 -b 0.0001 -o yzpa  -t ivnh > /tmp/bd_nhiv_63_2.log 2>&1
    /work/jzhu/project/finger/misc/bd.py -l 63  -d 2 -b 0.0001 -o yzpa  -t ivnh > /tmp/bd_nhiv_63_2.log 2>&1
    /work/jzhu/project/finger/misc/bd.py -l 126 -d 0 -b 0.0001 -o yzpa  -t ivnh > /tmp/bd_nhiv_126_0.log 2>&1
    /work/jzhu/project/finger/misc/bd.py -l 126 -d 2 -b 0.0001 -o yzpa  -t ivnh > /tmp/bd_nhiv_126_2.log 2>&1




    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.iv.1m -o > /tmp/calixew_nhiv1m.log 2>&1  
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.iv.6m -o > /tmp/calixew_nhiv6m.log 2>&1 
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.sk.1m -o > /tmp/calixew_nhsk1m.log 2>&1  
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.sk.6m -o > /tmp/calixew_nhsk6m.log 2>&1 
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.bfly.1m -o  > /tmp/calixew_nhbfly.1m.log 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.bfly.6m -o  > /tmp/calixew_nhbfly.6m.log 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.cs -o  > /tmp/calixew_nhcs.log 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.corr -o  > /tmp/calixew_nhcorr.log 2>&1

    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.iv.1m -o > /tmp/calixew_hz.iv.1m 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.iv.6m -o > /tmp/calixew_hz.iv.6m 2>&1 
    #/work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.bfly.1m -o
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.bfly.6m -o > /tmp/calixew_hz.bfly.6m 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.corr  -o > /tmp/calixew_hz.corr  2>&1
    #/work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.sk.1m  -o
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.sk.6m  -o > /tmp/calixew_hz.sk.6m 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.cs -o > /tmp/calixew_hz.cs    2>&1

    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.viv.1m -o   > /tmp/calixew_hz.viv.1m 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t hz.viv.6m -o   > /tmp/calixew_hz.viv.6m 2>&1   
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.viv.1m -o   > /tmp/calixew_nh.viv.1m 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_ixew -t nh.viv.6m -o   > /tmp/calixew_nh.viv.6m 2>&1



    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t dtta.sect.cov  -o  > /tmp/calcrv_dtta.sect.cov  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t cfsa.sect.cov  -o  > /tmp/calcrv_cfsa.sect.cov  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t hz.sect.cov  -o  > /tmp/calcrv_hz.sect.cov  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t nh.sect.cov  -o  > /tmp/calcrv_nh.sect.cov  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t spgs.sect.cov  -o  > /tmp/calcrv_spgs.sect.cov  2>&1


    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t dtta.sect.corr -o   > /tmp/calcrv_dtta.sect.corr 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t cfsa.sect.corr -o   > /tmp/calcrv_cfsa.sect.corr 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t hz.sect.corr -o   > /tmp/calcrv_hz.sect.corr 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t nh.sect.corr -o   > /tmp/calcrv_nh.sect.corr 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t spgs.sect.corr  -o  > /tmp/calcrv_spgs.sect.corr 2>&1

    /work/jzhu/project/zlib/zsprd.py -m cal_hr  -t iv_nh_hz -o  > /tmp/caliv_nh_hz.log 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_hr  -t iv_sp_hz -o  > /tmp/caliv_sp_hz.log  2>&1

    /work/jzhu/project/zlib/zsprd.py -m cal_csab -t hz.csab  -o  > /tmp/caliv_hz.csab.log 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_csab -t tf.csab  -o  > /tmp/caliv_tf.csab.log 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_csab -t nh.csab  -o  > /tmp/caliv_nh.csab.log 2>&1
 



    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/yf > /tmp/yf.log 2>&1
    /work/jzhu/project/finger/misc/futures_plot.py > /tmp/futures_plot.log 2>&1 
    /work/jzhu/project/finger/misc/futures_plot_bkdata.py > /tmp/bk_plot.log 2>&1 

    /work/jzhu/project/finger/misc/qb_plt.py -t nh -m cs -o -s 20200905  > /tmp/cs_nh.log 2>&1 
    /work/jzhu/project/finger/misc/qb_plt.py -t hz -m cs -o -s 20200905  > /tmp/cs_hz.log 2>&1


    /work/jzhu/project/finger/misc/qb_plt.py -t nh -o -f -m iv -s 20210108  > /tmp/iv_nh.log 2>&1  

    scp -rp /work/shared/output/complot* jzhu@123.57.60.6:/work/dwhang/project/sit/Shiny/yzpa/  > /tmp/scp_plt.log 2>&1 &
    scp -rp /work/shared/output/iv_*.pdf jzhu@123.57.60.6:/work/dwhang/project/sit/Shiny/yzpa/tmp/  > /tmp/scp_iv.log 2>&1 &


    /work/jzhu/project/finger/misc/pm.R > /tmp/pm.log.$edate 2>&1 
    scp -rp /work/shared/output/a_*pm.pdf jzhu@123.57.60.6:/work/dwhang/project/sit/Shiny/yzpa/ > /tmp/scp_pm.log 2>&1 &
    /work/jzhu/project/finger/misc/sa.pm  > /tmp/sa.log.$edate 2>&1
    scp -rp /work/shared/output/b_*sa.pdf jzhu@123.57.60.6:/work/dwhang/project/sit/Shiny/yzpa/

    /work/jzhu/project/finger/misc/patten_plot.py -t dtaa -o -s 20200905  > /tmp/tp_dtaa.log 2>&1 
    /work/jzhu/project/finger/misc/patten_plot.py -t dtaa -o -s 20200905  -b 50 -w 505  > /tmp/tp_dtaa_50_505.log 2>&1 
    /work/jzhu/project/finger/misc/patten_plot.py -t dtaa -o -s 20200905  -b 25  > /tmp/tp_dtaa_25.log 2>&1 


elif [ $d_type == 'gplt' ]; then
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/idxetf > /tmp/idp.log 2>&1

    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t gtaa.sect.cov  -o  > /tmp/calcrv_gtaa.sect.cov  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t dm.sect.cov  -o  > /tmp/calcrv_dm.sect.cov  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t em.sect.cov  -o  > /tmp/calcrv_em.sect.cov  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t icln.sect.cov  -o  > /tmp/calcrv_icln.sect.cov  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t xl.sect.cov  -o  > /tmp/calcrv_xl.sect.cov  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t xt.sect.cov  -o  > /tmp/calcrv_xt.sect.cov  2>&1

    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t gtaa.sect.corr  -o  > /tmp/calcrv_gtaa.sect.corr 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t dm.sect.corr  -o  > /tmp/calcrv_dm.sect.corr  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t em.sect.corr  -o  > /tmp/calcrv_em.sect.corr  2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t icln.sect.corr  -o  > /tmp/calcrv_icln.sect.corr 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t xl.sect.corr  -o  > /tmp/calcrv_xl.sect.corr 2>&1
    /work/jzhu/project/zlib/zsprd.py -m cal_crv -t xt.sect.corr  -o  > /tmp/calcrv_xt.sect.corr 2>&1

    /work/jzhu/project/finger/misc/qb_plt.py -t idxetf  -o -f > /tmp/qb_plt.log 2>&1  
    /work/jzhu/project/finger/misc/qb_plt.py -t idxetf   -o -f -m iv > /tmp/iv_ix.log 2>&1 
    /work/jzhu/project/finger/misc/qb_plt.py -t nh -m cs -o -s 20191205   > /tmp/nh_cs.log 2>&1
    /work/jzhu/project/finger/misc/qb_plt.py -t hz -m cs -o -s 20191205   > /tmp/hz_cs.log 2>&1

    cp -r /work/shared/output/complot* /work/dwhang/project/sit/Shiny/yzpa/
    cp -r /work/shared/output/iv_*.pdf /work/dwhang/project/sit/Shiny/yzpa/tmp/
    cp -r /work/shared/output/a_* /work/dwhang/project/sit/Shiny/yzpa/


elif [ $d_type == 'pi' ]; then
    scp -rp /work/jzhu/output/ql/mpa/*csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ 
    scp -rp /work/jzhu/output/dm/*csv  jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ 


elif [ $d_type == 'calm' ]; then
    /work/jzhu/project/ql/script/calm.py > /work/shared/daily/log/calm.log 2>&1
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/calm/CALM.cfpa > /work/shared/daily/log/calm.clog 2>&1
    scp -rp /work/shared/daily/ql/calm/CALM.wsign.cfpa.2022* jzhu@106.14.226.83:/work/shared/daily/ql/calm/ > /tmp/calm_scp.log


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
    /work/jzhu/project/ql/script/zmpa.py -t cfpa  -s 20160512 > /work/shared/daily/log/zmpa.cfpa.log 2>&1

    #/work/jzhu/project/ql/script/zmpa.py -t cfo2 -m ql/zmpa/LOZMPA -r w -s 20151212 > /work/shared/daily/log/lozmpa.cfo2.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t cfz2 -m ql/zmpa/LOZMPA -r w -s 20151212 > /work/shared/daily/log/lozmpa.cfz2.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t hack -m ql/zmpa/LOZMPA -r w -s 20210130 > /work/shared/daily/log/lozmpa.hack.log 2>&1
    /work/jzhu/project/ql/script/zmpa.py -t hzpa -m ql/zmpa/LOZMPA -r w > /work/shared/daily/log/lozmpa.hzpa.log 2>&1

    scp -rp /work/shared/daily/ql/zmpa/*2022* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 
    scp -rp /work/shared/daily/ql/zmpa/*2022* jzhu@123.57.60.6:/work/dwhang/project/sit/Shiny/yzpa/

    
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.cfpa > /work/shared/daily/log/zmpa.cfpa.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.cfsa > /work/shared/daily/log/zmpa.cfsa.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.cfca > /work/shared/daily/log/zmpa.cfca.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.ifpa > /work/shared/daily/log/zmpa.ifpa.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.tfpa > /work/shared/daily/log/zmpa.tfpa.clog 2>&1 &
    #/work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/LOZMPA.cfo2 > /work/shared/daily/log/zmpa.cfo2.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/LOZMPA.cfz2 > /work/shared/daily/log/zmpa.cfz2.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/LOZMPA.hzpa > /work/shared/daily/log/zmpa.hzpa.clog 2>&1 &

    scp -rp /work/jzhu/output/ql/zmpa/ZMPA.*.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/

elif [ $d_type == 'dmw' ]; then
    /work/jzhu/project/slib/script/mw.py -s 20210630 -t copc -o diff > /work/shared/daily/log/mw.co.diff.log 2>&1 
    /work/jzhu/project/slib/script/mw.py -s 20210630 -t cofu -o net  > /work/shared/daily/log/mw.cofu.net.log 2>&1 
    /work/jzhu/project/slib/script/mw.py -s 20210630 -t hzpc -o diff > /work/shared/daily/log/mw.hz.diff.log 2>&1 
    /work/jzhu/project/slib/script/mw.py -s 20210630 -t hzfu -o net  > /work/shared/daily/log/mw.hzfu.net.log 2>&1 
    /work/jzhu/project/slib/script/mw.py -s 20210630 -t tffu -o net  > /work/shared/daily/log/mw.tffu.net.log 2>&1 
    scp -rp /work/shared/daily/slib/mmw/*2022* jzhu@106.14.226.83:/work/shared/daily/slib/mmw/ 2>&1  
    scp -rp /work/shared/daily/slib/mmw/*cofu*2022* user1@8.142.157.170:/work/shared/daily/slib/mmw/ 2>&1  

    /work/jzhu/project/slib/script/mw.py -s 20210630 -t cofu -o all -f > /work/shared/daily/log/mw.cofu.all.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/mmw/ls.cofu.ew   > /work/shared/daily/log/lw.cflw.clog 2>&1 &

    /work/jzhu/project/slib/script/mw.py -s 20210630 -t tffu -o all -f > /work/shared/daily/log/mw.tffu.all.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/mmw/ls.tffu.ew   > /work/shared/daily/log/lw.tflw.clog 2>&1 &


elif [ $d_type == 'gmw' ]; then
    /work/jzhu/project/slib/script/mw.py -s 20210730 -t ixpc -o diff > /work/shared/daily/log/mw.ixpc.diff.log 2>&1 
    /work/jzhu/project/slib/script/mw.py -s 20210730 -t ixfu -o net > /work/shared/daily/log/mw.ixfu.net.log 2>&1 
    /work/jzhu/project/slib/script/mw.py -s 20210730 -t sppc -o diff > /work/shared/daily/log/mw.sppc.diff.log 2>&1 
    /work/jzhu/project/slib/script/mw.py -s 20210730 -t spfu -o net > /work/shared/daily/log/mw.spfu.diff.log 2>&1 
    scp -rp /work/shared/daily/slib/mmw/*202* jzhu@106.14.226.83:/work/shared/daily/slib/mmw/ 2>&1  

elif [ $d_type == 'dlw' ]; then
    /work/jzhu/project/ql/script/lw.py -m ql/ssl/ls -t cflw -r w -s 20160512 > /work/shared/daily/log/lw.ls.cflw.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/ssl/ls.cflw  > /work/shared/daily/log/lw.cflw.clog 2>&1 &
    scp -rp /work/jzhu/output/ql/ssl/ls.colw.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/

    #/work/jzhu/project/ql/script/lw.py -m ql/ssl/ls -t colw -r w -s 20160512 > /work/shared/daily/log/lw.ls.colw.log 2>&1 
    #/work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/ssl/ls.colw  > /work/shared/daily/log/lw.colw.clog 2>&1 &

    #/work/jzhu/project/ql/script/lw.py -m ql/ssl/ls -t hzlw -r w -s 20160512 > /work/shared/daily/log/lw.ls.hzlw.log 2>&1 
    #/work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/ssl/ls.hzlw  > /work/shared/daily/log/lw.hzlw.clog 2>&1 &
    /work/jzhu/project/ql/script/lw.py -m ql/ssl/lo -t hzlw -r w -s 20160512 > /work/shared/daily/log/lw.lo.hzlw.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/ssl/lo.hzlw  > /work/shared/daily/log/lo.hzlw.clog 2>&1 &

    scp -rp /work/jzhu/output/ql/ssl/lo.hzlw.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/

    /work/jzhu/project/ql/script/lw.py -m ql/ssl/ls -t tflw -r w -s 20160512 > /work/shared/daily/log/lw.ls.tflw.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/ssl/ls.tflw  > /work/shared/daily/log/ls.tflw.clog 2>&1 &
    scp -rp /work/jzhu/output/ql/ssl/ls.tflw.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/

elif [ $d_type == 'glw' ]; then
    /work/jzhu/project/ql/script/lw.py -m ql/ssl/ls -t ixlw -r w -s 20160512 > /work/shared/daily/log/lw.ls.ixlw.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/ssl/ls.ixlw  > /work/shared/daily/log/lw.ixlw.clog 2>&1 &

    /work/jzhu/project/ql/script/lw.py -m ql/ssl/ls -t splw -r w -s 20160512 > /work/shared/daily/log/lw.ls.splw.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/ssl/ls.splw  > /work/shared/daily/log/lw.splw.clog 2>&1 &

    cp /work/jzhu/output/ql/ssl/ls.colw.csv /work/jzhu/input/se2018/daily/


elif [ $d_type == 'ddma' ]; then
    /work/jzhu/project/ql/script/dma.py -t cfca -r w -s 20151212  > /work/shared/daily/log/dma.cfca.log 2>&1 
    /work/jzhu/project/ql/script/dma.py -t cfsa -r w -s 20151212  > /work/shared/daily/log/dma.cfsa.log 2>&1 
    /work/jzhu/project/ql/script/dma.py -t hzpa -r w -s 20151212  > /work/shared/daily/log/dma.cfsa.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/dma/DMA.ls.cfca > /work/shared/daily/log/dma.cfca.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/dma/DMA.ls.cfsa > /work/shared/daily/log/dma.cfsa.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/dma/DMA.ls.hzpa > /work/shared/daily/log/dma.hzpa.clog 2>&1 &

    /work/jzhu/project/ql/script/dgrid.py   -t cfca -r w > /work/shared/daily/log/dgrid.cfca.single.log 2>&1 
    /work/jzhu/project/ql/script/dgrid.py   -t CYNMSA.PO -r w -a -m ql/dma/grid > /work/shared/daily/log/dgrid.cfsa.single.log 2>&1 

elif [ $d_type == 'gdma' ]; then
    /work/jzhu/project/ql/script/dma.py -t spca   -r w -s 20151212 -l lo > /work/shared/daily/log/dma.idxcom.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/dma/DMA.lo.spca > /work/shared/daily/log/dma.idxcom.clog 2>&1 &


elif [ $d_type == 'drw' ]; then
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t cfpa -l 10
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t cfpa -l 20
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t cfpa -l 40
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t shsz -l 10
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t shsz -l 20
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t shsz -l 40

    /work/jzhu/project/slib/script/rw.py -m slib/lws/lo -t cfrw -r w -s 20180512 > /work/shared/daily/log/rw.lo.cfrw.log 2>&1 
    /work/jzhu/project/slib/script/rw.py -m slib/lws/so -t cfrw -r w -s 20180512 > /work/shared/daily/log/rw.so.cfrw.log 2>&1 

    /work/jzhu/project/slib/script/rw.py -m slib/lws/lo -t hzrw -r w -s 20060512 > /work/shared/daily/log/rw.lo.hzrw.log 2>&1 
    /work/jzhu/project/slib/script/rw.py -m slib/lws/lo -t tfrw -r w -s 20160512 > /work/shared/daily/log/rw.lo.tfrw.log 2>&1 

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/lws/lo.cfrw  > /work/shared/daily/log/rw.lo.cfrw.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/lws/so.cfrw  > /work/shared/daily/log/rw.so.cfrw.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/lws/lo.hzrw  > /work/shared/daily/log/rw.lo.hzrw.clog 2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/lws/lo.tfrw  > /work/shared/daily/log/rw.lo.tfrw.clog 2>&1 &




elif [ $d_type == 'grw' ]; then
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t spgs -l 10
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t spgs -l 20
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t spgs -l 40
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t idxetf -l 10
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t idxetf -l 20
    /work/jzhu/project/zlib/zstats.py -m cal_bb -o -t idxetf -l 40

    /work/jzhu/project/slib/script/rw.py -m slib/lws/lo -t ixrw -r w -s 20191212 > /work/shared/daily/log/rw.lo.ixrw.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/lws/lo.ixrw > /work/shared/daily/log/rw.lo.ixrw.clog 2>&1 &

    /work/jzhu/project/slib/script/rw.py -m slib/lws/lo -t sprw -r w -s 20151212 > /work/shared/daily/log/rw.lo.sprw.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/lws/lo.sprw > /work/shared/daily/log/rw.lo.sprw.clog 2>&1 &

    /work/jzhu/project/slib/script/rw.py -m slib/lws/lo -t etrw -r w -s 20191212 > /work/shared/daily/log/rw.lo.etrw.log 2>&1 

    /work/jzhu/project/slib/script/rwgrid.py -m slib/lws/single -t secetf  -l lo -x 4  > /tmp/sec.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/lws/single -t idxetf  > /tmp/idx.log 2>&1 &



elif [ $d_type == 'iv' ]; then
    scp -rp   user1@8.142.157.170:/work/shared/tradedata/SH/*22* /work/jzhu/input/tradedata/SH/
    scp -rp   user1@8.142.157.170:/work/shared/tradedata/SZ/*22* /work/jzhu/input/tradedata/SZ/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/CFE/*22* /work/jzhu/input/tradedata/CFE/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/CZC/*22* /work/jzhu/input/tradedata/CZC/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/DCE/*22* /work/jzhu/input/tradedata/DCE/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/INE/*22* /work/jzhu/input/tradedata/INE/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/SHF/*22* /work/jzhu/input/tradedata/SHF/

    scp -rp   user1@8.142.157.170:/work/shared/tradedata/SH/*23* /work/jzhu/input/tradedata/SH/
    scp -rp   user1@8.142.157.170:/work/shared/tradedata/SZ/*23* /work/jzhu/input/tradedata/SZ/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/CFE/*23* /work/jzhu/input/tradedata/CFE/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/CZC/*23* /work/jzhu/input/tradedata/CZC/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/DCE/*23* /work/jzhu/input/tradedata/DCE/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/INE/*23* /work/jzhu/input/tradedata/INE/
    scp -rp  user1@8.142.157.170:/work/shared/tradedata/SHF/*23* /work/jzhu/input/tradedata/SHF/

    scp -rp  user1@8.142.157.170:/work/shared/tradedata/ticker.csv /work/jzhu/input/tradedata/

    scp -rp  /work/jzhu/input/tradedata/*  jzhu@123.57.60.6:/work/dwhang/project/sit/Shiny/yzpa/tradedata

    #20220325 scp -rp 123.57.60.6:/work/jzhu/input/iv/*.csv /work/jzhu/input/iv/ > /tmp/iv.log 2>&1
    scp -rp /work/shared/daily/ql/zmpa/ZMPA.wsign.gloetf.* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 2>&1  
    scp -rp /work/shared/daily/ql/zmpa/ZMPA.wsign.secetf.* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 2>&1  
    scp -rp /work/shared/daily/ql/zmpa/ZMPA.wsign.cashetf.* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 2>&1 
    scp -rp /work/shared/daily/ql/zmpa/ZMPA.wsign.idxetf.* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 2>&1 

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.gloetf > /work/shared/daily/log/zmpa.gloetf.clog 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.secetf > /work/shared/daily/log/zmpa.secetf.clog
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.cashetf > /work/shared/daily/log/zmpa.cashetf.clog

    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t gloetf > /work/shared/daily/log/zmpa_gloetf_zgrid.log 2>&1 

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/single -t gloetf > /work/shared/daily/log/zmpa.gloetf.grid.log 

    /work/jzhu/project/slib/script/bby.py -t gloetf -w ew  -m slib/zbw/zmpa -s 20161127 > /tmp/zmpa_zbw_gloetf_ewvtb2.log 


elif [ $d_type == 'idxcom' ]; then
    #scp -rp 123.57.60.6:/work/jzhu/input/global/*.csv /work/jzhu/input/global/ > /tmp/global.scp.log 
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/global/  > /tmp/global.pol.log 

    /work/jzhu/project/ql/script/zmpa.py -t idxcom -s 20150512 > /work/shared/daily/log/zmpa.idxcom.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/ZMPA.idxcom > /work/shared/daily/log/zmpa.idxcom.clog  2>&1 
    scp -rp /work/shared/daily/ql/zmpa/ZMPA.wsign.idxcom.* jzhu@106.14.226.83:/work/shared/daily/ql/zmpa/ 

    /work/jzhu/project/ql/script/zgrid.py -m ql/zmpa/single -t idxcom > /work/shared/daily/log/zmpa_idxcom_zgrid.log 

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  ql/zmpa/single -t idxcom > /work/shared/daily/log/zmpa.idxcom.grid.log 

elif [ $d_type == 'doch' ]; then
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t cfpa > /tmp/chaodi_cfpa.log
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t shsz > /tmp/chaodi_shsz.log
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t iv > /tmp/chaodi_iv.log

    /work/jzhu/project/slib/script/kdj.py -t cflo -s 20200505 > /work/shared/daily/log/chaodi_cflo.log  2>&1
    /work/jzhu/project/slib/script/kdj.py -t colo -s 20180505 > /work/shared/daily/log/chaodi_colo.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -m slib/sw/so  -t coso -s 20180505 > /work/shared/daily/log/chaodi_coso.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -m slib/sw/so  -t hzso -s 20180505 > /work/shared/daily/log/chaodi_hzso.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -t hzlo -s 20060505 > /work/shared/daily/log/chaodi_hzlo.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -t tflo -s 20160505 > /work/shared/daily/log/chaodi_tflo.log  2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/jw/lo.cflo > /work/shared/daily/log/chaodi_cflo.clog  2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/jw/lo.colo > /work/shared/daily/log/chaodi_colo.clog  2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/sw/so.coso > /work/shared/daily/log/chaodi_coso.clog  2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/sw/so.hzso > /work/shared/daily/log/chaodi_hzso.clog  2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/jw/lo.hzlo > /work/shared/daily/log/chaodi_hzlo.clog  2>&1 &
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/jw/lo.tflo > /work/shared/daily/log/chaodi_tflo.clog  2>&1 &

    scp -rp /work/shared/daily/slib/fsd/jw*2021* jzhu@106.14.226.83:/work/shared/daily//slib/fsd/

elif [ $d_type == 'glch' ]; then
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t idxetf > /tmp/chaodi.log
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -o -t spgs > /tmp/chaodi.log
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -t dig -d dpi -o > /tmp/dig.dpi.log 2>&1
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -t dig -d iv30 -o > /tmp/dig.iv30.log 2>&1
    /work/jzhu/project/zlib/zstats.py -m cal_kdj -t dig -d gex -o > /tmp/dig.gex.log 2>&1

    /work/jzhu/project/slib/script/kdj.py -t qdii -s 20180505 > /work/shared/daily/log/chaodi_qdii.log  2>&1 

    /work/jzhu/project/slib/script/kdj.py -t splo -s 20150505 > /work/shared/daily/log/chaodi_splo.log  2>&1
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/jw/lo.splo > /work/shared/daily/log/chaodi_splo.clog  2>&1 &

    /work/jzhu/project/slib/script/kdj.py -t fasg -s 20180505 > /work/shared/daily/log/chaodi_fasg.log  2>&1
    /work/jzhu/project/slib/script/kdj.py -t slog -s 20180505 > /work/shared/daily/log/chaodi_slog.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -t morb -s 20180505 > /work/shared/daily/log/chaodi_morb.log  2>&1 
    /work/jzhu/project/slib/script/kdj.py -t idxetf -s 20180505 > /work/shared/daily/log/chaodi_idxetf.log  2>&1 

    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/jw/lo.idxetf >/work/shared/daily/log/chaodi_idxetf.clog  2>&1 &
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
    /work/jzhu/project/slib/script/bby.py -t cfpa -w ewvt  -m slib/zbw/zmpa -s 20160512 > /tmp/zmpa_zbw_cfpa_ewvt.log 2>&1 
    /work/jzhu/project/slib/script/bby.py -t ifpa -w ewb2  -m slib/zbw/zmpa  -s 20160512 > /tmp/zmpa_zbw_ifpa_ewb2.log 2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/zbw/zmpa.ifpa.ewb2 > /work/shared/daily/log/zmpa_ifpa.ewb2_pk_to_csv.log  2>&1 
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  slib/zbw/zmpa.tfpa.ewvtb2 > /work/shared/daily/log/zmpa_tfpa.ewvtb2_pk_to_csv.log  2>&1 
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
    scp -rp 123.57.60.6:/tmp/macro/*csv /work/jzhu/data/raw
    #20220325 scp -rp 123.57.60.6:/work/jzhu/input/idxetf/*csv /work/jzhu/input/idxetf/ 
    #scp -rp /work/jzhu/input/idxetf/*.csv 123.57.60.6:/work/jzhu/input/idxetf/
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/idxetf > /tmp/idp.log 2>&1

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
    cp /work/jzhu/output/macro/M1ADVPPI.csv /work/jzhu/project/ql/data/

    #/work/jzhu/project/ql/script/stockmacroih.py -e $edate  >  /work/shared/daily/log/stockmacroih.log 2>&1
    /work/jzhu/project/ql/script/hzmacro.py -s 20160101 -r w  >  /work/shared/daily/log/hzmacro.log 2>&1
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  /ql/mac/macro.hzpa > /tmp/macro.hzpa.log 

    #/work/jzhu/project/ql/script/comacro.py -s 20160101 -e $edate >  /work/shared/daily/log/comacro.log 2>&1
    /work/jzhu/project/ql/script/comacro.py -s 20160101 -r w >  /work/shared/daily/log/comacro.log 2>&1
    /work/jzhu/project/slib/script/pickle_to_csv.py -m  /ql/mac/macro.cfca > /tmp/macro.cfca.log 

elif [ $d_type == 'usmacro' ]; then
    scp -rp 123.57.60.6:/tmp/US_macro/*csv /work/jzhu/data/raw
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/idxetf > /tmp/idp.log 2>&1
    /work/jzhu/project/zdata/csvpolish.py -i /work/jzhu/input/global > /tmp/idp.log 2>&1
    #cp /work/jzhu/data/raw/pmi2022-04-30.csv /work/jzhu/data/raw/pmi.csv
    /work/jzhu/project/zlib/zsprd.py -m cal_usmacro -o > /tmp/cal_macro.log 2>&1 
    cp /work/jzhu/output/macro/us_ODSCHG.csv /work/jzhu/project/ql/data/
 




elif [ $d_type == 'scp' ]; then

    scp /work/jzhu/data/pol/Index/*.csv 106.14.226.83:/work/shared/data/pol/Index/
    scp /work/jzhu/data/pol/Index/*.csv 123.57.60.6:/work/jzhu/data/pol/Index/
    scp /work/jzhu/input/index/sql/szse/daily/399300.SZ.csv 123.57.60.6:/work/jzhu/input/index/sql/szse/daily/
    #scp /work/jzhu/data/pol/work/jzhu/input/global/*.csv 123.57.60.6:/work/jzhu/data/pol/work/jzhu/input/global/
    #scp /work/jzhu/data/pol/work/jzhu/input/idxetf/*.csv 123.57.60.6:/work/jzhu/data/pol/work/jzhu/input/idxetf/

    scp -rp /work/jzhu/input/yf/000300.SH.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/
    scp -rp /work/jzhu/input/yf/399006.SZ.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/
    scp -rp /work/jzhu/input/yf/000016.SH.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/

    scp -rp /work/jzhu/output/ql/mpa/single/*.MPA.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/
    scp -rp /work/jzhu/output/ql/mpa/single/*.MPA.csv jzhu@106.14.226.83:/work/shared/daily/ql/mpa/single/

    scp -rp /work/jzhu/output/slib/bbx/mpa.ewvt.csv jzhu@123.57.60.6:/work/jzhu/input/se2018/daily/ > /tmp/mpa_scp.log 
    scp -rp /work/jzhu/output/slib/bbx/mpa.ewvt.csv jzhu@106.14.226.83:/work/jzhu/input/se2018/daily/ > /tmp/mpa_scp.log 

    scp -rp /work/jzhu/input/idxetf user1@8.142.157.170:/work/shared/Index/

elif [ $d_type == 'yfscp' ]; then
    scp -rp jzhu@123.57.60.6:/work/shared/daily/slib/jw/*qdii* user1@8.142.157.170:/work/shared/daily/slib/mmw/




else
    echo "WARNING: wrong data type $d_type"
fi 
exit 0







