#!/bin/bash

v_file_route=`sh /data2/USERS/hadoop_dfgx_A/zk/20180112-shenzhengtrip/shell/shenzhen/file.sh`
v_file=$v_file_route/dfgx_sz_sn_province_travel_more_one_day.txt

hive -e "
	select first_day,round(count(*)/0.17) from dfgx_tour_db.dfgx_sz_province_travel_2018gq where stay_day!=0 group by first_day order by first_day;
">$v_file