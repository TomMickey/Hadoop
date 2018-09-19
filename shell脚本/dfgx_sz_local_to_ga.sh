#!/bin/bash

v_file_route=`sh /data2/USERS/hadoop_dfgx_A/zk/20180112-shenzhengtrip/shell/shenzhen/file.sh`
v_file=$v_file_route/dfgx_sz_local_to_ga.txt

hive -e "
	select start_day,round(count(*)/0.17) from dfgx_tour_db.dfgx_sz_local_to_ga_2018gq group by start_day order by start_day;
">$v_file