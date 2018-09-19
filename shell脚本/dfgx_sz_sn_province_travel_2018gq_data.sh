#!/bin/bash

v_file_route=`sh /data2/USERS/hadoop_dfgx_A/zk/20180112-shenzhengtrip/shell/shenzhen/file.sh`
v_file=$v_file_route/dfgx_sz_sn_province_travel_2018gq.txt

hive -e "
	select b.city,b.traffic_type,b.stay_day,b.sex,b.age_type,round(count(*)/0.17) from (select a.age_type,a.isdn,a.city,case when a.final_traffic_type=1 then '飞机' when a.final_traffic_type=2 then '火车 ' when a.final_traffic_type=3 then ' 汽车' else '未知' end as traffic_type,a.stay_day,case when a.sex like '%M%' then '男' when a.sex like '%F%' then '女' else '未知' end as sex from dfgx_tour_db.dfgx_sz_sn_province_travel_2018gq a) b group by b.city,b.traffic_type,b.stay_day,b.sex,b.age_type;
">$v_file