#!/bin/bash

v_file_route=`sh /data2/USERS/hadoop_dfgx_A/zk/20180112-shenzhengtrip/shell/shenzhen/file.sh`
v_file=$v_file_route/hot-paihang.txt

spark-sql --master yarn --queue root.B --num-executors 400 --executor-cores 3 --executor-memory 10G -e "
	set mapreduce.job.name='热门商圈、景区排行-统计排行';
	select a.spot_name,round(count(distinct isdn)/0.17) ant from (select spot_name,substr(t_time,1,8) as day_time,isdn from dfgx_tour_db.dfgx_sz_hotviews_2018gq group by spot_name,substr(t_time,1,8),isdn) a group by a.spot_name order by ant desc;
">$v_file

