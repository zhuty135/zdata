# zdata
整个历史模式：（-f表示full history, -a 表示附加模式，如果没有的话，会drop 整个table）
例子1：nohup  ./tutosql.py -d stock -f -a -o > /tmp/testfund.log 2>&1 &
 

回填模式，就是抓取过去5天的数据，写如数据库。 
/work/jzhu/project/gitrepos/zdata/tutosql.py -d index -n -5 -o
备注：-o 表示会写入数据库或硬盘，缺失不写入
